import asyncio
import websockets
import json
import time
from typing import List, Dict

from c_log import UnifiedLogger
from symbols import PhemexSymbols

logger = UnifiedLogger("ws_screener", context="Screener")

class PhemexWSScreener:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
            
        self.ws_url = "wss://phemex.com/ws"
        self.check_interval = self.config.get("check_interval", 60)
        self.threshold = float(self.config.get("oi_threshold_percent", 95.0))
        self.cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        
        self.target_symbols: List[str] = []
        self.oi_limits: Dict[str, float] = {}
        self.signal_cache: Dict[str, float] = {}
        
        self._first_ws_logged = False
        self._first_rest_logged = False
        
        self.sym_api = PhemexSymbols()

    async def _refresh_limits_loop(self):
        """Регулярно обновляем лимиты (и символы) через REST каждые check_interval секунд"""
        logger.info(f"Запуск REST-воркера для обновления лимитов (интервал: {self.check_interval}с)")
        while True:
            try:
                symbols_info = await self.sym_api.get_all()
                new_symbols = []
                
                for si in symbols_info:
                    sym = si.symbol
                    raw = si.raw_data
                    new_symbols.append(sym)
                    
                    if not self._first_rest_logged:
                        logger.info(f"[DEBUG REST] Пример данных по символу {sym}: {json.dumps(raw)}")
                        self._first_rest_logged = True
                        
                    # Phemex не отдает 'maxOpenInterest' явно. Мы парсим потенциальные поля лимитов.
                    # maxRiskPositionRv - макс. позиция риска
                    # Если в логах REST [DEBUG REST] вы увидите другой точный ключ — просто добавьте его сюда.
                    limit_str = (
                        raw.get("maxOpenInterest") or 
                        raw.get("maxRiskPositionRv") or 
                        raw.get("maxOrderQtyRq") or
                        raw.get("maxOrderQtyRv") or
                        0
                    )
                    
                    try:
                        limit_val = float(limit_str)
                        if limit_val > 0:
                            self.oi_limits[sym] = limit_val
                    except (ValueError, TypeError):
                        pass

                if not self.target_symbols:
                    self.target_symbols = new_symbols
                    logger.info(f"Найдено {len(self.target_symbols)} символов.")
                    
            except Exception as e:
                logger.error(f"Ошибка обновления лимитов: {e}")
                
            await asyncio.sleep(self.check_interval)

    async def _ping_loop(self, ws, chunk_id):
        """Автопинг для поддержания соединения WS (Phemex требует его)"""
        while True:
            await asyncio.sleep(15)
            try:
                ping_msg = {"id": chunk_id, "method": "server.ping", "params": []}
                await ws.send(json.dumps(ping_msg))
            except Exception:
                break

    def _cleanup_cache(self):
        """Очистка устаревших сигналов из кеша для восстановления чувствительности"""
        now = time.time()
        expired = [sym for sym, ts in self.signal_cache.items() if now - ts >= self.cooldown_sec]
        for sym in expired:
            del self.signal_cache[sym]

    async def _process_msg(self, raw_msg: str, chunk_id: int):
        try:
            data = json.loads(raw_msg)
            
            # Логируем полностью первый пришедший словарь тиков для сверки полей
            if not self._first_ws_logged and "tick" in data:
                logger.info(f"[DEBUG WS] Пример tick-данных: {json.dumps(data)}")
                self._first_ws_logged = True

            if "tick" in data:
                t = data["tick"]
                symbol = t.get("symbol")
                if not symbol: return
                
                # Ищем текущий открытый интерес (openInterestRv - строковое значение в USDT, openInterest - сырое)
                oi_str = t.get("openInterestRv")
                if oi_str is None:
                    oi_str = t.get("openInterest", 0)
                    
                current_oi = float(oi_str)
                limit = self.oi_limits.get(symbol, 0)
                
                if limit > 0:
                    usage = (current_oi / limit) * 100
                    
                    if usage >= self.threshold:
                        self._trigger_signal(symbol, current_oi, limit, usage)
                        
            elif "error" in data and data.get("error"):
                logger.error(f"[WS Chunk {chunk_id}] Ошибка биржи: {data['error']}")
                
        except Exception as e:
            logger.error(f"[WS Chunk {chunk_id}] Ошибка парсинга: {e}")

    def _trigger_signal(self, symbol: str, current_oi: float, limit: float, usage: float):
        self._cleanup_cache()
        
        now = time.time()
        if symbol not in self.signal_cache:
            logger.warning(f"🚨 [LIMIT EXCEEDED] {symbol} | Загрузка: {usage:.2f}% | OI: {current_oi} / Limit: {limit}")
            self.signal_cache[symbol] = now

    async def ws_worker(self, symbols_chunk: List[str], chunk_id: int):
        """Воркер для подключения к WS чанка (набора) символов"""
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://phemex.com"
        }
        
        while True:
            try:
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=None) as ws:
                    logger.info(f"[WS Chunk {chunk_id}] Соединение установлено. Символов в батче: {len(symbols_chunk)}")
                    
                    sub_msg = {
                        "id": chunk_id,
                        "method": "tick.subscribe",
                        "params": symbols_chunk
                    }
                    await ws.send(json.dumps(sub_msg))
                    
                    ping_task = asyncio.create_task(self._ping_loop(ws, chunk_id))
                    
                    try:
                        while True:
                            raw_data = await ws.recv()
                            await self._process_msg(raw_data, chunk_id)
                    except websockets.ConnectionClosed:
                        logger.warning(f"[WS Chunk {chunk_id}] Соединение закрыто. Переподключение...")
                    finally:
                        ping_task.cancel()
                        
            except Exception as e:
                logger.error(f"[WS Chunk {chunk_id}] Ошибка WS: {e}. Рестарт через 5 сек...")
                await asyncio.sleep(5)

    async def run(self):
        logger.info(f"=== Запуск Screener Phemex | Порог OI: {self.threshold}% | Cooldown: {self.cooldown_sec}с ===")
        
        # 1. Запуск регулярного обновления лимитов по REST
        rest_task = asyncio.create_task(self._refresh_limits_loop())
        
        # 2. Ожидание первичной инициализации символов
        while not self.target_symbols:
            await asyncio.sleep(1)
            
        # 3. Разбивка на чанки по 30 символов для снижения нагрузки на канал
        chunk_size = 30
        chunks = [self.target_symbols[i:i + chunk_size] for i in range(0, len(self.target_symbols), chunk_size)]
        
        logger.info(f"Создано {len(chunks)} WS чанков для подписки на {len(self.target_symbols)} символов.")
        
        ws_tasks = [asyncio.create_task(self.ws_worker(chunk, i + 1)) for i, chunk in enumerate(chunks)]
        
        await asyncio.gather(rest_task, *ws_tasks)


if __name__ == "__main__":
    screener = PhemexWSScreener()
    try:
        asyncio.run(screener.run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен.")