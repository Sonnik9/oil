import asyncio
import time
import json
from typing import Any, Dict, Optional
import aiohttp

from c_log import UnifiedLogger
from symbols import PhemexSymbols

logger = UnifiedLogger("rest_screener", context="Screener")

class PhemexRESTScreener:
    BASE_URL = "https://api.phemex.com"

    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
            
        self.check_interval = self.config.get("check_interval", 60)
        self.threshold = float(self.config.get("oi_threshold_percent", 45.0))
        self.cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        
        self.oi_limits: Dict[str, float] = {}
        self.signal_cache: Dict[str, float] = {}
        self.sym_api = PhemexSymbols()
        
        self._timeout = aiohttp.ClientTimeout(total=20.0)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        
        self._first_ticker_dumped = False

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
        last_err: Exception | None = None
        for attempt in range(1, 4):
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}: {text}")
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict) and int(data.get("code", 0)) != 0:
                        raise RuntimeError(f"code={data.get('code')} msg={data.get('msg')}")
                    return data
            except Exception as exc:
                last_err = exc
                await asyncio.sleep(0.5 * attempt)
        raise RuntimeError(f"Phemex request failed: {path} err={last_err}")

    async def _refresh_limits(self):
        try:
            symbols_info = await self.sym_api.get_all()
            for si in symbols_info:
                sym = si.symbol
                raw = si.raw_data
                
                if raw.get("status") != "Listed":
                    self.oi_limits.pop(sym, None)
                    continue
                    
                limit_str = raw.get("maxOI", 0)
                if float(limit_str) <= 0:
                    limit_str = raw.get("maxOrderQtyRq", 0)
                
                try:
                    limit_val = float(limit_str)
                    if limit_val > 0:
                        self.oi_limits[sym] = limit_val
                except (ValueError, TypeError):
                    pass

            # ВОЗВРАЩАЕМ ДАМП ЛИМИТОВ
            with open("phemex_limits.json", "w", encoding="utf-8") as f:
                json.dump(self.oi_limits, f, indent=4)

            logger.info(f"Обновлены лимиты для {len(self.oi_limits)} символов. Файл phemex_limits.json обновлен.")
        except Exception as e:
            logger.error(f"Ошибка обновления лимитов: {e}")

    def _cleanup_cache(self):
        now = time.time()
        expired = [sym for sym, ts in self.signal_cache.items() if now - ts >= self.cooldown_sec]
        for sym in expired:
            del self.signal_cache[sym]

    async def poll_tickers_loop(self):
        logger.info(f"=== Запуск REST-скринера | Порог: {self.threshold}% | Интервал: {self.check_interval}с ===")
        
        while True:
            start_t = time.time()
            try:
                data = await self._get_json("/md/ticker/24hr/all")
                
                # ЖЕСТКИЙ ДАМП ПЕРВОГО ОТВЕТА
                if not self._first_ticker_dumped:
                    with open("phemex_raw_tickers.json", "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)
                    logger.info("✅ СЫРОЙ ОТВЕТ С ТИКЕРАМИ СОХРАНЕН В phemex_raw_tickers.json!")
                    self._first_ticker_dumped = True
                
                payload = data.get("result") or data.get("data") or []
                items = payload if isinstance(payload, list) else [payload]
                
                found_signals = 0
                for item in items:
                    sym = item.get("symbol")
                    if not sym or sym not in self.oi_limits:
                        continue
                        
                    oi_str = item.get("openInterestRv") or item.get("openInterest") or item.get("openInterestValue") or 0
                    current_oi = float(oi_str)
                    limit = self.oi_limits.get(sym, 0)
                    
                    if limit > 0 and current_oi > 0:
                        usage = (current_oi / limit) * 100
                        if usage >= self.threshold:
                            self._trigger_signal(sym, current_oi, limit, usage)
                            found_signals += 1
                            
                # ПУЛЬС: Всегда выводим результат опроса
                logger.info(f"Опрос завершен (тиков: {len(items)}). Сигналов: {found_signals}. Спим...")
                
            except Exception as e:
                logger.error(f"Ошибка массового опроса тикеров: {e}")
                
            elapsed = time.time() - start_t
            sleep_time = max(1.0, self.check_interval - elapsed)
            await asyncio.sleep(sleep_time)

    def _trigger_signal(self, symbol: str, current_oi: float, limit: float, usage: float):
        self._cleanup_cache()
        now = time.time()
        if symbol not in self.signal_cache:
            logger.warning(f"🚨 [LIMIT EXCEEDED] {symbol} | Загрузка: {usage:.2f}% | OI: {current_oi} / Limit: {limit}")
            self.signal_cache[symbol] = now

    async def run(self):
        await self._refresh_limits()
        
        async def limits_worker():
            while True:
                await asyncio.sleep(3600)
                await self._refresh_limits()
                
        asyncio.create_task(limits_worker())
        await self.poll_tickers_loop()

if __name__ == "__main__":
    screener = PhemexRESTScreener()
    try:
        asyncio.run(screener.run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен.")