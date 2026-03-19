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
    KUCOIN_RAW_FILE = 'kucoin_raw_contracts.json'

    def __init__(self, config_path='config.json'):
        # Загружаем конфиг
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except Exception:
            self.config = {}

        # 100% — ОИ Phemex равен лимиту KuCoin
        self.fill_threshold = float(self.config.get("oi_spike_threshold_percent", 100.0))
        self.check_interval = self.config.get("check_interval", 60)
        self.signal_cooldown_sec = self.config.get("signal_cooldown_sec", 300)
        
        self.signal_cache: Dict[str, float] = {}
        self.phemex_contract_sizes: Dict[str, float] = {}
        self.kucoin_base_limits: Dict[str, float] = {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self._session

    async def sync_kucoin_limits(self):
        """Парсер KuCoin: обновляет лимиты, если файл старый или его нет"""
        need_update = True
        if os.path.exists(self.KUCOIN_RAW_FILE):
            if time.time() - os.path.getmtime(self.KUCOIN_RAW_FILE) < 86400:
                need_update = False

        if need_update:
            logger.info("Обновление справочника KuCoin через API...")
            session = await self._get_session()
            try:
                async with session.get(f"{self.BASE_KUCOIN}/api/v1/contracts/active") as resp:
                    data = await resp.json()
                    if data.get("code") == "200000":
                        with open(self.KUCOIN_RAW_FILE, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4)
                        logger.info("Справочник KuCoin сохранен.")
            except Exception as e:
                logger.error(f"Ошибка обновления KuCoin: {e}")

    async def load_metadata(self):
        """Загрузка всех метаданных в память"""
        # 1. Phemex: контрактные размеры через symbols.py
        try:
            ph_sym = PhemexSymbols()
            symbols = await ph_sym.get_all()
            for s in symbols:
                # Достаем число из строки типа "0.001 BTC" или "1.0"
                c_size_str = str(s.raw_data.get("contractSize", "1"))
                match = re.search(r"[\d\.]+", c_size_str)
                self.phemex_contract_sizes[s.symbol] = float(match.group()) if match else 1.0
            await ph_sym.aclose()
        except Exception as e:
            logger.error(f"Ошибка Phemex symbols: {e}")

        # 2. KuCoin: лимиты в монетах из локального файла
        try:
            with open(self.KUCOIN_RAW_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f).get("data", [])
            for item in data:
                base = item.get("symbol", "").replace("USDTM", "").replace("USDT", "")
                if base == "XBT": base = "BTC"
                # Лимит в штуках монет = лимит * множитель
                limit = float(item.get("maxRiskLimit", 0)) * float(item.get("multiplier", 1))
                if limit > 0:
                    self.kucoin_base_limits[base] = limit
            logger.info(f"Загружено лимитов KuCoin: {len(self.kucoin_base_limits)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки лимитов: {e}")

    async def run(self):
        await self.sync_kucoin_limits()
        await self.load_metadata()

        if not self.kucoin_base_limits:
            logger.error("Нет данных для сравнения. Скринер остановлен.")
            return

        logger.info(f"=== OIL-Скринер v5 | Порог: {self.fill_threshold}% ===")
        session = await self._get_session()
        
        while True:
            start_t = time.time()
            try:
                # Опрос Phemex
                async with session.get(f"{self.BASE_PHEMEX}/md/v2/ticker/24hr/all") as resp:
                    data = await resp.json()
                    for item in (data.get("data", []) or data.get("result", [])):
                        sym = item.get("symbol", "")
                        if not sym or not sym.endswith("USDT"): continue
                        
                        limit_coins = self.kucoin_base_limits.get(sym.replace("USDT", ""))
                        if not limit_coins: continue

                        # Считаем чистые монеты ОИ
                        oi_contracts = float(item.get("openInterest", 0))
                        ph_size = self.phemex_contract_sizes.get(sym, 1.0)
                        phemex_coins = oi_contracts * ph_size
                        
                        # Утилизация лимита в %
                        ratio = (phemex_coins / limit_coins) * 100.0
                        
                        if ratio >= self.fill_threshold:
                            now = time.time()
                            if sym not in self.signal_cache or (now - self.signal_cache[sym] > self.signal_cooldown_sec):
                                # Цена для красивого вывода в $
                                raw_price = float(item.get("markPriceRp", 0) or item.get("lastRp", 0))
                                price = raw_price / 10000.0
                                
                                logger.warning(
                                    f"🚨 [OIL] {sym:10} | Заполнено: {ratio:6.1f}% | "
                                    f"Phemex: ${phemex_coins*price:,.0f} | KuCoin Limit: ${limit_coins*price:,.0f}"
                                )
                                self.signal_cache[sym] = now
            except Exception as e:
                logger.error(f"Ошибка опроса: {e}")
            
            await asyncio.sleep(max(1, self.check_interval - (time.time() - start_t)))

if __name__ == "__main__":
    try:
        asyncio.run(PhemexOILScreener().run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен.")