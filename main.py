from __future__ import annotations

import asyncio
import contextlib
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import aiohttp
from aiohttp import WSMsgType

from c_log import UnifiedLogger
from symbols import PhemexSymbols, ProductsCatalog


logger = UnifiedLogger("ws_screener", context="OIScreener")


def chunked(values: Sequence[str], size: int) -> List[List[str]]:
    batch_size = max(1, int(size))
    return [list(values[idx: idx + batch_size]) for idx in range(0, len(values), batch_size)]


class PhemexOpenInterestScreener:
    BASE_URL = "https://api.phemex.com"
    WS_URL = "wss://ws.phemex.com"

    LIMIT_KEYS = (
        "maxOI",
        "maxOi",
        "maxOpenInterest",
        "maxOpenInterestRv",
        "maxOpenInterestRq",
        "openInterestLimit",
        "openInterestLimitRv",
        "openInterestLimitRq",
        "openInterestCap",
    )

    def __init__(self, config_path: str = "config.json"):
        with open(config_path, "r", encoding="utf-8") as file_obj:
            self.config = json.load(file_obj)

        self.api_url = self.config.get("api_url", self.BASE_URL).rstrip("/")
        self.ws_url = self.config.get("ws_url", self.WS_URL)
        self.check_interval = int(self.config.get("check_interval", 60))
        self.threshold_percent = float(self.config.get("oi_threshold_percent", 95.0))
        self.cooldown_sec = int(self.config.get("signal_cooldown_sec", 300))
        self.ws_ping_interval_sec = int(self.config.get("ws_ping_interval_sec", 20))
        self.ws_message_timeout_sec = int(self.config.get("ws_message_timeout_sec", 35))
        self.ws_reconnect_delay_sec = float(self.config.get("ws_reconnect_delay_sec", 3.0))
        self.ws_backoff_max_sec = float(self.config.get("ws_backoff_max_sec", 30.0))
        self.symbol_batch_size = int(self.config.get("symbol_batch_size", 50))

        self.oi_limits: Dict[str, float] = {}
        self.limit_sources: Dict[str, str] = {}
        self.signal_cache: Dict[str, float] = {}
        self.allowed_symbols: List[str] = []
        self.symbol_batches: List[List[str]] = []

        self.products_client = PhemexSymbols(base_url=self.api_url)
        self._timeout = aiohttp.ClientTimeout(total=30.0)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()

        self._first_products_dumped = False
        self._first_market_dumped = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
            return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        await self.products_client.aclose()

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number

    @staticmethod
    def _save_json(path: str, payload: Any) -> None:
        Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def _rebuild_symbol_batches(self) -> None:
        self.symbol_batches = chunked(self.allowed_symbols, self.symbol_batch_size)
        logger.debug(
            "Сформированы батчи символов: %s батч(ей), размер батча=%s",
            len(self.symbol_batches),
            self.symbol_batch_size,
        )

    def _extract_limit_from_product(self, product: Dict[str, Any], root_data: Dict[str, Any]) -> tuple[Optional[float], str]:
        for key in self.LIMIT_KEYS:
            value = self._to_float(product.get(key))
            if value is not None:
                return value, f"product.{key}"

        leverage_margin_id = product.get("leverageMargin") or product.get("leverageMarginId") or product.get("leverage_margin")
        leverage_margins = root_data.get("leverageMargins") or []
        if leverage_margin_id is not None and isinstance(leverage_margins, list):
            for margin in leverage_margins:
                if not isinstance(margin, dict):
                    continue
                if margin.get("index_id") != leverage_margin_id:
                    continue
                items = margin.get("items") or []
                if not isinstance(items, list) or not items:
                    continue
                last_item = items[-1] if isinstance(items[-1], dict) else None
                if last_item is None:
                    continue
                fallback = self._to_float(last_item.get("notionalValueRv"))
                if fallback is not None:
                    return fallback, f"leverageMargins[{leverage_margin_id}].items[-1].notionalValueRv"

        return None, "missing"

    async def refresh_limits(self) -> None:
        catalog: ProductsCatalog = await self.products_client.get_catalog(quote="USDT", only_active=True)
        root_data = catalog.root_data
        symbols = sorted({info.symbol for info in catalog.symbols})
        next_limits: Dict[str, float] = {}
        next_sources: Dict[str, str] = {}
        skipped_symbols: List[str] = []

        if not self._first_products_dumped:
            self._save_json("phemex_products_raw.json", root_data)
            logger.log_json("debug", "Сырой ответ /public/products сохранен в phemex_products_raw.json", root_data)
            self._first_products_dumped = True

        for info in catalog.symbols:
            limit_value, limit_source = self._extract_limit_from_product(info.raw_data, root_data)
            if limit_value is None:
                skipped_symbols.append(info.symbol)
                continue
            next_limits[info.symbol] = limit_value
            next_sources[info.symbol] = limit_source

        self.allowed_symbols = sorted(next_limits)
        self.oi_limits = next_limits
        self.limit_sources = next_sources
        self._rebuild_symbol_batches()

        self._save_json("phemex_limits.json", self.oi_limits)
        self._save_json(
            "phemex_limits_debug.json",
            {
                "symbols_from_products": symbols,
                "parsed_limits": self.oi_limits,
                "limit_sources": self.limit_sources,
                "skipped_symbols": skipped_symbols,
            },
        )

        logger.info(
            "Лимиты OI обновлены: symbols=%s, with_limits=%s, skipped=%s, refresh_interval=%ss",
            len(symbols),
            len(self.oi_limits),
            len(skipped_symbols),
            self.check_interval,
        )
        if skipped_symbols:
            logger.warning("Не удалось извлечь лимиты OI для %s символов: %s", len(skipped_symbols), ", ".join(skipped_symbols[:20]))

    def _cleanup_cache(self) -> None:
        now = time.time()
        expired = [symbol for symbol, ts in self.signal_cache.items() if now - ts >= self.cooldown_sec]
        for symbol in expired:
            self.signal_cache.pop(symbol, None)
            logger.debug("Символ %s удален из cooldown-кеша", symbol)

    def _should_emit_signal(self, symbol: str) -> bool:
        self._cleanup_cache()
        return symbol not in self.signal_cache

    def _cache_signal(self, symbol: str) -> None:
        self.signal_cache[symbol] = time.time()

    def _trigger_signal(self, symbol: str, current_oi: float, limit: float) -> None:
        usage = (current_oi / limit) * 100
        if not self._should_emit_signal(symbol):
            logger.debug("Повторный сигнал по %s подавлен cooldown=%ss", symbol, self.cooldown_sec)
            return

        logger.warning(
            "🚨 [LIMIT EXCEEDED] %s | usage=%.2f%% | current_oi=%s | limit=%s | threshold=%.2f%% | source=%s",
            symbol,
            usage,
            current_oi,
            limit,
            self.threshold_percent,
            self.limit_sources.get(symbol, "unknown"),
        )
        self._cache_signal(symbol)

    def _map_market_rows(self, payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        fields = payload.get("fields") or []
        rows = payload.get("data") or []
        mapped: Dict[str, Dict[str, Any]] = {}
        if not isinstance(fields, list) or not isinstance(rows, list):
            return mapped
        for row in rows:
            if not isinstance(row, list) or len(row) != len(fields):
                continue
            item = dict(zip(fields, row))
            symbol = item.get("symbol")
            if isinstance(symbol, str) and symbol in self.oi_limits:
                mapped[symbol] = item
        return mapped

    def _process_market_payload(self, payload: Dict[str, Any]) -> None:
        if not self._first_market_dumped:
            self._save_json("phemex_ws_market24h_raw.json", payload)
            logger.log_json("debug", "Сырой ответ websocket сохранен в phemex_ws_market24h_raw.json", payload)
            self._first_market_dumped = True

        market_rows = self._map_market_rows(payload)
        total_processed = 0
        found_signals = 0

        for batch in self.symbol_batches:
            for symbol in batch:
                item = market_rows.get(symbol)
                if not item:
                    continue
                total_processed += 1
                current_oi = self._to_float(item.get("openInterestRv"))
                limit = self.oi_limits.get(symbol)
                if current_oi is None or limit is None or limit <= 0:
                    continue
                usage = (current_oi / limit) * 100
                if usage >= self.threshold_percent:
                    self._trigger_signal(symbol, current_oi, limit)
                    found_signals += 1

        logger.info(
            "Обработан websocket-пакет: rows=%s, matched=%s, signals=%s",
            len(payload.get("data") or []),
            total_processed,
            found_signals,
        )

    async def _limits_refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.check_interval)
            try:
                await self.refresh_limits()
            except Exception:
                logger.exception("Ошибка фонового обновления лимитов OI")

    async def _subscribe_market_feed(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        subscribe_payload = {
            "id": int(time.time() * 1000),
            "method": "perp_market24h_pack_p.subscribe",
            "params": [],
        }
        await ws.send_json(subscribe_payload)
        logger.info("Отправлена подписка на perp_market24h_pack_p.subscribe")

    async def _handle_ws_message(self, msg: aiohttp.WSMessage) -> None:
        if msg.type == WSMsgType.TEXT:
            payload = json.loads(msg.data)
            if payload.get("error"):
                logger.error("WebSocket error payload: %s", payload)
                return
            method = payload.get("method")
            if method == "perp_market24h_pack_p.update":
                self._process_market_payload(payload)
            elif payload.get("result"):
                logger.debug("Подтверждение websocket команды: %s", payload)
            else:
                logger.debug("Пропущено websocket-сообщение без market update: %s", payload)
            return

        if msg.type == WSMsgType.PING:
            logger.debug("Получен websocket ping")
            return
        if msg.type == WSMsgType.PONG:
            logger.debug("Получен websocket pong")
            return
        if msg.type == WSMsgType.CLOSE:
            raise ConnectionError("WebSocket close frame received")
        if msg.type == WSMsgType.CLOSED:
            raise ConnectionError("WebSocket connection closed")
        if msg.type == WSMsgType.ERROR:
            raise ConnectionError(f"WebSocket transport error: {ws_exception_str(msg)}")

    async def websocket_loop(self) -> None:
        reconnect_delay = self.ws_reconnect_delay_sec
        while True:
            try:
                session = await self._get_session()
                logger.info(
                    "Подключение к websocket %s | heartbeat=%ss | timeout=%ss",
                    self.ws_url,
                    self.ws_ping_interval_sec,
                    self.ws_message_timeout_sec,
                )
                async with session.ws_connect(
                    self.ws_url,
                    heartbeat=self.ws_ping_interval_sec,
                    autoping=True,
                    receive_timeout=self.ws_message_timeout_sec,
                    max_msg_size=0,
                ) as ws:
                    await self._subscribe_market_feed(ws)
                    reconnect_delay = self.ws_reconnect_delay_sec
                    while True:
                        msg = await ws.receive()
                        await self._handle_ws_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("WebSocket loop упал, реконнект через %.1fс", reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(self.ws_backoff_max_sec, max(1.0, reconnect_delay * 2))

    async def run(self) -> None:
        logger.info(
            "=== Запуск OI-скринера | threshold=%.2f%% | cooldown=%ss | limits_refresh=%ss ===",
            self.threshold_percent,
            self.cooldown_sec,
            self.check_interval,
        )
        await self.refresh_limits()
        refresh_task = asyncio.create_task(self._limits_refresh_loop(), name="limits-refresh-loop")
        try:
            await self.websocket_loop()
        finally:
            refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await refresh_task
            await self.close()


def ws_exception_str(msg: aiohttp.WSMessage) -> str:
    exc = msg.data if isinstance(msg.data, BaseException) else None
    return repr(exc) if exc is not None else "unknown websocket error"


if __name__ == "__main__":
    screener = PhemexOpenInterestScreener()
    try:
        asyncio.run(screener.run())
    except KeyboardInterrupt:
        logger.info("Скринер остановлен вручную.")
