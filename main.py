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
            
        self.ws_url = "wss://ws.phemex.com"
        self.check_interval = self.config.get("check_interval", 60)
        self.threshold = float(self.config.get("oi_threshold_percent", 45.0))
        self.cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        
        self.target_symbols: List[str] = []
        self.oi_limits: Dict[str, float] = {}
        self.signal_cache: Dict[str, float] = {}
        
        self._spam_counter = 0
        self.sym_api = PhemexSymbols()

    async def _refresh_limits_loop(self):
        """Регулярно обновляем лимиты (и символы) через REST"""
        logger.info(f"Запуск REST-воркера для обновления лимитов (интервал: {self.check_interval}с)")
        while True:
            try:
                symbols_info = await self.sym_api.get_all()
                new_symbols = []
                raw_rest_dump = {}
                
                for si in symbols_info:
                    sym = si.symbol
                    raw = si.raw_data
                    new_symbols.append(sym)
                    raw_rest_dump[sym] = raw
                    
                    limit_str = raw.get("maxOI", 0)
                    if float(limit_str) <= 0:
                        limit_str = raw.get("maxOrderQtyRq", 0)
                    
                    try:
                        limit_val = float(limit_str)
                        if limit_val > 0:
                            self.oi_limits[sym] = limit_val
                    except (ValueError, TypeError):
                        pass

                # === СОХРАНЯЕМ В JSON СРАЗУ ЖЕ ===
                with open("phemex_limits.json", "w", encoding="utf-8") as f:
                    json.dump(self.oi_limits, f, indent=4)
                    
                with open("phemex_raw_rest.json", "w", encoding="utf-8") as f:
                    json.dump(raw_rest_dump, f, indent=4)
                    
                logger.info("✅ Лимиты записаны в phemex_limits.json | Сырые данные в phemex_raw_rest.json")

                if not self.target_symbols:
                    self.target_symbols = new_symbols
                    logger.info(f"Найдено {len(self.target_symbols)} символов.")
                    
            except Exception as e:
                logger.error(f"Ошибка обновления лимитов: {e}")
                
            await asyncio.sleep(self.check_interval)

    async def _ping_loop(self, ws, chunk_id):
        while True:
            await asyncio.sleep(15)
            try:
                await ws.send(json.dumps({"id": chunk_id, "method": "server.ping", "params": []}))
            except Exception:
                break

    def _cleanup_cache(self):
        now = time.time()
        expired = [sym for sym, ts in self.signal_cache.items() if now - ts >= self.cooldown_sec]
        for sym in expired:
            del self.signal_cache[sym]

    async def _process_msg(self, raw_msg: str, chunk_id: int):
        try:
            data = json.loads(raw_msg)
            
            # СПАМИМ ПЕРВЫЕ 30 ПАКЕТОВ В ЛОГ ДЛЯ АНАЛИЗА КЛЮЧЕЙ
            if self._spam_counter < 30 and "result" not in data and "error" not in data:
                logger.info(f"[WS RAW SPAM] {json.dumps(data)}")
                self._spam_counter += 1

            payload = data.get("market24h") or data.get("tick") or data.get("spot_market24h")
            
            if payload:
                items = payload if isinstance(payload, list) else [payload]
                for item in items:
                    symbol = item.get("symbol")
                    if not symbol: 
                        continue
                    
                    # Пытаемся вытащить OI по всем известным ключам
                    oi_str = item.get("openInterestRv") or item.get("openInterest") or item.get("openInterestValue") or 0
                        
                    current_oi = float(oi_str)
                    limit = self.oi_limits.get(symbol, 0)
                    
                    if limit > 0 and current_oi > 0:
                        usage = (current_oi / limit) * 100
                        if usage >= self.threshold:
                            self._trigger_signal(symbol, current_oi, limit, usage)
                            
            elif "error" in data and data.get("error"):
                logger.error(f"[WS Chunk {chunk_id}] Ошибка биржи: {data['error']}")
                
        except Exception as e:
            pass # Игнорим ошибки парсинга мусора, чтобы не засорять лог

    async def ws_worker(self, symbols_chunk: List[str], chunk_id: int):
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://phemex.com"
        }
        
        while True:
            try:
                async with websockets.connect(self.ws_url, extra_headers=headers, ping_interval=None) as ws:
                    logger.info(f"[WS Chunk {chunk_id}] Соединение установлено. Подписка ПОШТУЧНО...")
                    
                    # ПОШТУЧНАЯ ПОДПИСКА (чтобы одна монета не сломала весь батч)
                    for i, sym in enumerate(symbols_chunk):
                        sub_msg = {
                            "id": chunk_id * 1000 + i,
                            "method": "tick.subscribe",
                            "params": [sym]
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.05) # Небольшой таймаут между подписками
                        
                    # ТЕСТ: В первом чанке дополнительно запрашиваем вообще весь рынок без параметров
                    if chunk_id == 1:
                        test_msg = {
                            "id": 99999,
                            "method": "market24h.subscribe",
                            "params": []
                        }
                        await ws.send(json.dumps(test_msg))
                    
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

    def _trigger_signal(self, symbol: str, current_oi: float, limit: float, usage: float):
        self._cleanup_cache()
        now = time.time()
        if symbol not in self.signal_cache:
            logger.warning(f"🚨 [LIMIT EXCEEDED] {symbol} | Загрузка: {usage:.2f}% | OI: {current_oi} / Limit: {limit}")
            self.signal_cache[symbol] = now

    async def run(self):
        logger.info(f"=== Запуск Screener Phemex | Порог OI: {self.threshold}% | Cooldown: {self.cooldown_sec}с ===")
        rest_task = asyncio.create_task(self._refresh_limits_loop())
        
        while not self.target_symbols:
            await asyncio.sleep(1)
            
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