import asyncio
import time
import json
from typing import Any, Dict, Optional
from collections import deque
import aiohttp

from c_log import UnifiedLogger

logger = UnifiedLogger("delta_screener", context="Screener")

class PhemexDeltaScreener:
    BASE_URL = "https://api.phemex.com"

    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
            
        self.check_interval = self.config.get("check_interval", 60)
        self.spike_threshold = float(self.config.get("oi_spike_threshold_percent", 3.0))
        self.cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        self.window_size = int(self.config.get("window_size_ticks", 5))
        
        # Хранилище за историята на ОИ: символ -> deque (опашка с фиксиран размер)
        self.oi_history: Dict[str, deque] = {}
        self.signal_cache: Dict[str, float] = {}
        
        self._timeout = aiohttp.ClientTimeout(total=20.0)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()

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
        expired = [sym for sym, ts in self.signal_cache.items() if now - ts >= self.cooldown_sec]
        for sym in expired:
            del self.signal_cache[sym]

    async def poll_tickers_loop(self):
        logger.info(f"=== Запуск Делта-Скрийнер Phemex | Праг на скок: +{self.spike_threshold}% | Окно: {self.window_size} тика ===")
        
        while True:
            start_t = time.time()
            try:
                data = await self._get_json("/md/v2/ticker/24hr/all")
                payload = data.get("result") or data.get("data") or []
                items = payload if isinstance(payload, list) else [payload]
                
                found_signals = 0
                for item in items:
                    sym = item.get("symbol")
                    # Филтрираме само USDT фючърси
                    if not sym or not sym.endswith("USDT"):
                        continue
                        
                    oi_str = item.get("openInterestRv") or item.get("openInterest") or 0
                    current_oi = float(oi_str)
                    
                    if current_oi <= 0:
                        continue
                        
                    # Инициализираме историята за нови монети
                    if sym not in self.oi_history:
                        self.oi_history[sym] = deque(maxlen=self.window_size)
                        
                    history = self.oi_history[sym]
                    
                    # Ако вече имаме натрупана история (поне 1 стар запис)
                    if len(history) > 0:
                        # Взимаме най-старото налично ОИ в нашия прозорец
                        old_oi = history[0]
                        
                        if old_oi > 0:
                            delta_percent = ((current_oi - old_oi) / old_oi) * 100.0
                            
                            # Проверяваме за рязък скок
                            if delta_percent >= self.spike_threshold:
                                self._trigger_signal(sym, current_oi, old_oi, delta_percent)
                                found_signals += 1
                                
                    # Добавяме текущата стойност към прозореца
                    history.append(current_oi)
                            
                logger.info(f"Опрос Phemex завършен (тикове: {len(items)}). Сигнали: {found_signals}. Спя...")
                
            except Exception as e:
                logger.error(f"Грешка при масов опрос: {e}")
                
            elapsed = time.time() - start_t
            sleep_time = max(1.0, self.check_interval - elapsed)
            await asyncio.sleep(sleep_time)

    def _trigger_signal(self, symbol: str, current_oi: float, old_oi: float, delta: float):
        self._cleanup_cache()
        now = time.time()
        if symbol not in self.signal_cache:
            logger.warning(f"🚀 [ВСПЛЕСК ОИ] {symbol} | Скок: +{delta:.2f}% | Беше: {old_oi:.0f} -> Сега: {current_oi:.0f}")
            self.signal_cache[symbol] = now

    async def run(self):
        await self.poll_tickers_loop()

if __name__ == "__main__":
    screener = PhemexDeltaScreener()
    try:
        asyncio.run(screener.run())
    except KeyboardInterrupt:
        logger.info("Скрийнерът е спрян.")