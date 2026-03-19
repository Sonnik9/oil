import asyncio
import time
import json
import re
from typing import Any, Dict, Optional
import aiohttp

from c_log import UnifiedLogger
from symbols import PhemexSymbols

logger = UnifiedLogger("oil_screener", context="Screener")

class PhemexOILScreener:
    BASE_URL = "https://api.phemex.com"

    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
            
        # Порог аномалии. Поставь в конфиге адекватное значение, например 300.0 или 500.0
        self.fill_threshold = float(self.config.get("oi_spike_threshold_percent", 500.0))
            
        self.check_interval = self.config.get("check_interval", 60)
        self.signal_cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        self.signal_cache: Dict[str, float] = {}
        
        self._timeout = aiohttp.ClientTimeout(total=20.0)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        
        self.phemex_contract_sizes: Dict[str, float] = {}
        self.kucoin_base_limits: Dict[str, float] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
            return self._session

    async def _get_json(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict) and int(data.get("code", 0)) != 0:
                        raise RuntimeError(f"code={data.get('code')} msg={data.get('msg')}")
                    return data
            except Exception as exc:
                last_err = exc
                await asyncio.sleep(0.5 * attempt)
        raise RuntimeError(f"Phemex request failed: {path} err={last_err}")

    def _cleanup_cache(self):
        now = time.time()
        expired = [sym for sym, ts in self.signal_cache.items() if now - ts >= self.signal_cooldown_sec]
        for sym in expired:
            del self.signal_cache[sym]

    async def load_metadata(self):
        # 1. Тянем размеры контрактов Phemex
        try:
            ph_sym = PhemexSymbols()
            symbols = await ph_sym.get_all()
            for s in symbols:
                c_size_str = str(s.raw_data.get("contractSize", "1"))
                match = re.search(r"[\d\.]+", c_size_str)
                if match:
                    self.phemex_contract_sizes[s.symbol] = float(match.group())
                else:
                    self.phemex_contract_sizes[s.symbol] = 1.0
            await ph_sym.aclose()
        except Exception as e:
            logger.error(f"Ошибка загрузки символов Phemex: {e}")

        # 2. Высчитываем лимиты KuCoin в БАЗОВЫХ МОНЕТАХ раз и навсегда напрямую из RAW
        try:
            with open('kucoin_raw_contracts.json', 'r', encoding='utf-8') as f:
                kucoin_raw = json.load(f)
                
            items = kucoin_raw.get("data", []) if isinstance(kucoin_raw, dict) and "data" in kucoin_raw else kucoin_raw
            if isinstance(items, dict):
                items = items.values()
                
            loaded = 0
            for item in items:
                if not isinstance(item, dict): continue
                sym = item.get("symbol", "")
                base = sym.replace("USDTM", "").replace("USDT", "")
                if base == "XBT": base = "BTC"
                
                limit_contracts = float(item.get("maxRiskLimit", 0.0))
                mult = float(item.get("multiplier", 1.0))
                
                # Лимит в чистых монетах (например, 250 BTC)
                limit_base_coins = limit_contracts * mult
                if limit_base_coins > 0:
                    self.kucoin_base_limits[base] = limit_base_coins
                    loaded += 1
            logger.info(f"Загружены лимиты KuCoin (в базовых монетах): {loaded} шт.")
        except Exception as e:
            logger.error(f"Ошибка загрузки kucoin_raw_contracts: {e}")

    def get_kucoin_limit(self, symbol: str) -> float:
        base_sym = symbol.replace("USDT", "")
        if base_sym in self.kucoin_base_limits:
            return self.kucoin_base_limits[base_sym]
        # Откусываем префиксы щитков, если они есть
        clean = re.sub(r'^(U|u)?(1000000|10000|1000|100|1M|1m)', '', base_sym)
        if clean in self.kucoin_base_limits:
            return self.kucoin_base_limits[clean]
        return 0.0

    async def poll_tickers_loop(self):
        await self.load_metadata()
        logger.info("=== Запуск VIBE-Скринера v3 | ЧИСТАЯ МАТЕМАТИКА | Базовые монеты ===")
        logger.info(f"Порог аномалии: {self.fill_threshold}% от лимита старшего брата")
        
        while True:
            start_t = time.time()
            try:
                data = await self._get_json("/md/v2/ticker/24hr/all")
                payload = data.get("result") or data.get("data") or []
                items = payload if isinstance(payload, list) else [payload]
                
                found_signals = 0
                for item in items:
                    sym = item.get("symbol")
                    if not sym or not sym.endswith("USDT"):
                        continue
                        
                    # 1. Эталонный лимит Кукоина (в штуках монет)
                    kucoin_limit_base_coins = self.get_kucoin_limit(sym)
                    if kucoin_limit_base_coins <= 0:
                        continue
                        
                    # Больше не доверяем openInterestRv, берем сырые контракты!
                    oi_contracts_str = item.get("openInterest")
                    price_str = item.get("markPriceRp")
                    
                    if not oi_contracts_str or not price_str:
                        continue
                        
                    current_oi_contracts = float(oi_contracts_str)
                    
                    # 2. ОИ Фемекса (в штуках монет)
                    ph_contract_size = self.phemex_contract_sizes.get(sym, 1.0)
                    phemex_oi_base_coins = current_oi_contracts * ph_contract_size
                    
                    if phemex_oi_base_coins <= 0: continue
                    
                    # 3. Идеальная метрика: Монеты делим на Монеты (цена вообще не участвует в логике)
                    fill_ratio = (phemex_oi_base_coins / kucoin_limit_base_coins) * 100.0
                    
                    if fill_ratio >= self.fill_threshold:
                        # Доллары считаем только для красоты в логе.
                        # Внимание: у Фемекса цена в API умножена на 10000, поэтому делим обратно.
                        current_price = float(price_str) / 10000.0
                        phemex_usdt = phemex_oi_base_coins * current_price
                        kucoin_usdt = kucoin_limit_base_coins * current_price
                        
                        self._trigger_signal(sym, phemex_usdt, kucoin_usdt, fill_ratio)
                        found_signals += 1
                            
                logger.info(f"Опрос Phemex завершен (тикеров: {len(items)}). Сигналов: {found_signals}. Сплю...")
                
            except Exception as e:
                logger.error(f"Ошибка при опросе: {e}")
                
            elapsed = time.time() - start_t
            sleep_time = max(1.0, self.check_interval - elapsed)
            await asyncio.sleep(sleep_time)

    def _trigger_signal(self, symbol: str, phemex_usdt: float, kucoin_usdt: float, ratio: float):
        self._cleanup_cache()
        now = time.time()
        if symbol not in self.signal_cache:
            logger.warning(f"🚨 [ПЕРЕГРУЗ ОИ] {symbol} | Phemex: ${phemex_usdt:,.0f} | KuCoin Limit: ${kucoin_usdt:,.0f} | Заполнение: {ratio:.1f}%")
            self.signal_cache[symbol] = now

    async def run(self):
        try:
            await self.poll_tickers_loop()
        finally:
            # Корректно закрываем сессию aiohttp, чтобы не было ругани при остановке
            if self._session and not self._session.closed:
                await self._session.close()
            await asyncio.sleep(0.1)

if __name__ == "__main__":
    screener = PhemexOILScreener()
    try:
        asyncio.run(screener.run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен.")