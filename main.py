import asyncio
import websockets
import json
import time
from typing import Dict

from c_log import UnifiedLogger
from symbols import PhemexSymbols

logger = UnifiedLogger("ws_screener", context="Screener")

class PhemexWSScreener:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
            
        self.ws_url = "wss://ws.phemex.com"
        self.check_interval = self.config.get("check_interval", 60)
        self.threshold = float(self.config.get("oi_threshold_percent", 95.0)) # Возвращаем рабочий порог
        self.cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        
        self.oi_limits: Dict[str, float] = {}
        self.signal_cache: Dict[str, float] = {}
        self.sym_api = PhemexSymbols()

    async def _refresh_limits_loop(self):
        """Регулярно обновляем лимиты через REST"""
        logger.info(f"Запуск REST-воркера для обновления лимитов (интервал: {self.check_interval}с)")
        while True:
            try:
                symbols_info = await self.sym_api.get_all()
                for si in symbols_info:
                    sym = si.symbol
                    raw = si.raw_data
                    
                    # Проверка доступности торгов по статусу (вместо description)
                    if raw.get("status") != "Listed":
                        if sym in self.oi_limits:
                            del self.oi_limits[sym]
                        continue
                        
                    # Phemex отдает maxOI = -1.0 для безлимита, используем фолбэк
                    limit_str = raw.get("maxOI", 0)
                    if float(limit_str) <= 0:
                        limit_str = raw.get("maxOrderQtyRq", 0)
                    
                    try:
                        limit_val = float(limit_str)
                        if limit_val > 0:
                            self.oi_limits[sym] = limit_val
                    except (ValueError, TypeError):
                        pass

                logger.info(f"Обновлены лимиты для {len(self.oi_limits)} активных символов.")
            except Exception as e:
                logger.error(f"Ошибка обновления лимитов: {e}")
                
            await asyncio.sleep(self.check_interval)

    async def _ping_loop(self, ws):
        while True:
            await asyncio.sleep(15)
            try:
                await ws.send(json.dumps({"id": 0, "method": "server.ping", "params": []}))
            except Exception:
                break

    def _cleanup_cache(self):
        now = time.time()
        expired = [sym for sym, ts in self.signal_cache.items() if now - ts >= self.cooldown_sec]
        for sym in expired:
            del self.signal_cache[sym]

    async def ws_worker(self):
        """ЕДИНОЕ ГЛОБАЛЬНОЕ СОЕДИНЕНИЕ: подписка на весь рынок сразу"""
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://phemex.com"
        }
        
        while True:
            try:
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=None) as ws:
                    logger.info("Глобальное WS соединение установлено. Запрашиваем поток всего рынка...")
                    
                    # Передаем пустой params: [] чтобы Phemex отдал все монеты
                    sub_msg = {
                        "id": 1,
                        "method": "market24h.subscribe",
                        "params": []
                    }
                    await ws.send(json.dumps(sub_msg))
                    
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    
                    try:
                        while True:
                            raw_data = await ws.recv()
                            await self._process_msg(raw_data)
                    except websockets.ConnectionClosed:
                        logger.warning("WS Соединение закрыто. Переподключение...")
                    finally:
                        ping_task.cancel()
                        
            except Exception as e:
                logger.error(f"Ошибка глобального WS: {e}. Рестарт через 5 сек...")
                await asyncio.sleep(5)

    async def _process_msg(self, raw_msg: str):
        try:
            data = json.loads(raw_msg)
            
            payload = data.get("market24h")
            if payload:
                items = payload if isinstance(payload, list) else [payload]
                for item in items:
                    symbol = item.get("symbol")
                    if not symbol: 
                        continue
                    
                    # openInterest приходит напрямую в контрактах
                    oi_str = item.get("openInterest", 0)
                    current_oi = float(oi_str)
                    limit = self.oi_limits.get(symbol, 0)
                    
                    if limit > 0 and current_oi > 0:
                        usage = (current_oi / limit) * 100
                        if usage >= self.threshold:
                            self._trigger_signal(symbol, current_oi, limit, usage)
                            
            elif "error" in data and data.get("error"):
                logger.error(f"Ошибка биржи: {data['error']}")
                
        except Exception:
            pass

    def _trigger_signal(self, symbol: str, current_oi: float, limit: float, usage: float):
        self._cleanup_cache()
        now = time.time()
        if symbol not in self.signal_cache:
            logger.warning(f"🚨 [LIMIT EXCEEDED] {symbol} | Загрузка: {usage:.2f}% | OI: {current_oi} / Limit: {limit}")
            self.signal_cache[symbol] = now

    async def run(self):
        logger.info(f"=== Запуск Screener Phemex | Порог OI: {self.threshold}% | Cooldown: {self.cooldown_sec}с ===")
        rest_task = asyncio.create_task(self._refresh_limits_loop())
        
        while not self.oi_limits:
            await asyncio.sleep(1)
            
        logger.info(f"Лимиты загружены. Запуск глобального WebSocket потока.")
        ws_task = asyncio.create_task(self.ws_worker())
        
        await asyncio.gather(rest_task, ws_task)

if __name__ == "__main__":
    screener = PhemexWSScreener()
    try:
        asyncio.run(screener.run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен.")