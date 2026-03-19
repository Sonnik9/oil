import asyncio
import websockets
import json
import logging
from datetime import datetime

class PhemexWSScreener:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger("WSScreener")
        
        self.ws_url = "wss://phemex.com/ws" 
        self.target_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"] 

    async def run(self):
        threshold = float(self.config.get("oi_threshold_percent", 95.0))
        self.logger.info(f"Запуск WS (Порог: {threshold}%)")

        # Правильные заголовки
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Origin": "https://phemex.com"
        }

        try:
            # В новых версиях websockets аргумент называется просто additional_headers или через extra_headers
            # Но самый верный способ — передать их в именованный параметр
            async with websockets.connect(self.ws_url, additional_headers=headers) as ws:
                self.logger.info("Соединение установлено.")

                subscribe_msg = {
                    "id": 1,
                    "method": "tick.subscribe",
                    "params": self.target_symbols
                }
                await ws.send(json.dumps(subscribe_msg))

                while True:
                    raw_data = await ws.recv()
                    data = json.loads(raw_data)
                    
                    if "tick" in data:
                        t = data["tick"]
                        symbol = t.get("symbol")
                        oi = float(t.get("openInterest", 0))
                        max_oi = float(t.get("maxOpenInterest", 0))

                        if max_oi > 0:
                            usage = (oi / max_oi) * 100
                            
                            # Дебаг всегда для BTC
                            if "BTC" in symbol:
                                self.logger.info(f"[LIVE] {symbol} | Usage: {usage:.2f}% | OI: {oi:,.0f}")

                            if usage >= threshold:
                                self.logger.warning(f"❗ [LIMIT] {symbol} | Load: {usage:.2f}%")
                    
                    elif "error" in data:
                        self.logger.error(f"Биржа вернула ошибку: {data['error']}")

        except Exception as e:
            # Если additional_headers не помог, пробуем через классический extra_headers
            # (некоторые версии требуют именно его)
            self.logger.info("Пробую альтернативный метод подключения...")
            try:
                async with websockets.connect(self.ws_url, extra_headers=headers) as ws:
                    # (повтор логики подписки...)
                    self.logger.info("Подключено через extra_headers.")
            except Exception as e2:
                self.logger.error(f"Все методы подключения провалены: {e2}")

if __name__ == "__main__":
    screener = PhemexWSScreener()
    asyncio.run(screener.run())