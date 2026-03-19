import asyncio
import time
import json
import re
import os
from typing import Any, Dict, Optional
import aiohttp

from c_log import UnifiedLogger
from symbols import PhemexSymbols

logger = UnifiedLogger("oil_screener", context="Screener")

class PhemexOILScreener:
    BASE_PHEMEX = "https://api.phemex.com"
    BASE_KUCOIN = "https://api-futures.kucoin.com"
    KUCOIN_FILE = 'kucoin_raw_contracts.json'

    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
            
        self.fill_threshold = float(self.config.get("oi_spike_threshold_percent", 100.0))
        self.check_interval = self.config.get("check_interval", 60)
        self.signal_cooldown_sec = self.config.get("signal_cooldown_sec", 600)
        
        self.signal_cache: Dict[str, float] = {}
        self.phemex_metadata: Dict[str, Dict] = {}
        self.kucoin_base_limits: Dict[str, float] = {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25))
        return self._session

    def clean_symbol(self, symbol: str) -> str:
        """Очищает тикер от префиксов (1000, 1000000, USDT, M) для сопоставления бирж"""
        s = symbol.upper().replace("USDT", "").replace("USDTM", "")
        s = re.sub(r'^(1000000|100000|10000|1000|100|10|U|X|1M)', '', s)
        if s == "XBT": return "BTC"
        return s

    async def sync_kucoin_data(self):
        """Парсер KuCoin: качает лимиты, если файл старый или его нет"""
        if not os.path.exists(self.KUCOIN_FILE) or (time.time() - os.path.getmtime(self.KUCOIN_FILE) > 86400):
            logger.info("Обновление справочника KuCoin через API...")
            session = await self._get_session()
            try:
                async with session.get(f"{self.BASE_KUCOIN}/api/v1/contracts/active") as resp:
                    data = await resp.json()
                    if data.get("code") == "200000":
                        with open(self.KUCOIN_FILE, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4)
                        logger.info("Справочник KuCoin обновлен.")
            except Exception as e:
                logger.error(f"Ошибка KuCoin API: {e}")

    async def load_metadata_bridge(self):
        """Строит мост соответствий: сводит лимиты и размеры к чистым монетам"""
        # 1. Phemex: размеры контрактов и шкала цены
        try:
            ph_sym = PhemexSymbols()
            symbols = await ph_sym.get_all()
            for s in symbols:
                size_match = re.search(r"[\d\.]+", str(s.raw_data.get("contractSize", "1")))
                self.phemex_metadata[s.symbol] = {
                    "size": float(size_match.group()) if size_match else 1.0,
                    "price_scale": int(s.raw_data.get("priceScale", 4))
                }
            await ph_sym.aclose()
        except Exception as e:
            logger.error(f"Ошибка метаданных Phemex: {e}")

        # 2. KuCoin: лимиты в чистых монетах
        try:
            with open(self.KUCOIN_FILE, 'r', encoding='utf-8') as f:
                ku_data = json.load(f).get("data", [])
            for item in ku_data:
                clean_name = self.clean_symbol(item.get("symbol", ""))
                # Лимит в чистых монетах = контракты * множитель
                limit = float(item.get("maxRiskLimit", 0)) * float(item.get("multiplier", 1))
                if limit > 0:
                    self.kucoin_base_limits[clean_name] = limit
            logger.info(f"Мост построен: {len(self.kucoin_base_limits)} лимитов синхронизировано.")
        except Exception as e:
            logger.error(f"Ошибка загрузки KuCoin: {e}")

    async def poll_tickers_loop(self):
        await self.sync_kucoin_data()
        await self.load_metadata_bridge()
        
        session = await self._get_session()
        logger.info(f"=== OIL-Скринер ЗАПУЩЕН | Порог: {self.fill_threshold}% ===")
        
        while True:
            start_t = time.time()
            try:
                async with session.get(f"{self.BASE_PHEMEX}/md/v2/ticker/24hr/all") as resp:
                    payload = await resp.json()
                    items = payload.get("data", []) or payload.get("result", [])
                    
                    for item in items:
                        sym = item.get("symbol", "")
                        if not sym or not sym.endswith("USDT"): continue
                        
                        clean_name = self.clean_symbol(sym)
                        limit_coins = self.kucoin_base_limits.get(clean_name)
                        if not limit_coins: continue

                        # Считаем объем Phemex в чистых монетах
                        meta = self.phemex_metadata.get(sym, {"size": 1.0, "price_scale": 4})
                        phemex_coins = float(item.get("openInterest", 0)) * meta["size"]
                        
                        # % утилизации лимита
                        ratio = (phemex_coins / limit_coins) * 100.0
                        
                        if ratio >= self.fill_threshold:
                            now = time.time()
                            if sym not in self.signal_cache or (now - self.signal_cache[sym] > self.signal_cooldown_sec):
                                # Получаем цену (с учетом масштаба биржи)
                                raw_price = float(item.get("markPriceRp", 0) or item.get("lastRp", 0))
                                price = raw_price / (10 ** meta["price_scale"])
                                
                                logger.warning(
                                    f"🚨 [OIL] {sym:12} | Заполнение: {ratio:7.1f}% | "
                                    f"Phemex: ${phemex_coins*price:,.0f} | KuLimit: ${limit_coins*price:,.0f}"
                                )
                                self.signal_cache[sym] = now
            except Exception as e:
                logger.error(f"Ошибка цикла: {e}")
            
            await asyncio.sleep(max(1, self.check_interval - (time.time() - start_t)))

    async def run(self):
        try:
            await self.poll_tickers_loop()
        finally:
            if self._session: await self._session.close()

if __name__ == "__main__":
    try:
        asyncio.run(PhemexOILScreener().run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен.")