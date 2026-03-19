# ============================================================
# FILE: symbol.py
# ROLE: Phemex USDT Perpetual (Futures) symbols via REST.
# NOTE: Standalone module (no package imports).
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    status: str
    quote: str
    raw_data: Dict[str, Any]


@dataclass(frozen=True)
class ProductsCatalog:
    root_data: Dict[str, Any]
    symbols: List[SymbolInfo]


class PhemexSymbols:
    """Public symbols REST.

    Uses:
        GET https://api.phemex.com/public/products
    """

    BASE_URL = "https://api.phemex.com"

    def __init__(self, timeout_sec: float = 20.0, retries: int = 3, base_url: str | None = None):
        self.BASE_URL = (base_url or self.BASE_URL).rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
        self._retries = int(retries)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
            return self._session

    async def aclose(self) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    async def _get_json(self, path: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(1, self._retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}: {text}")
                    data = await resp.json(content_type=None)
                    if not isinstance(data, dict):
                        raise RuntimeError(f"Bad JSON root: {type(data)}")
                    if int(data.get("code", 0)) != 0:
                        raise RuntimeError(f"API error code={data.get('code')} msg={data.get('msg')}")
                    return data
            except Exception as e:
                last_err = e
                s = (str(e) or "").lower()
                if "session is closed" in s or "connector is closed" in s or "clientconnectorerror" in s:
                    self._session = None
                if attempt < self._retries:
                    await asyncio.sleep(0.4 * attempt)
                else:
                    break
        raise RuntimeError(f"Phemex symbols failed: {path} err={last_err}")

    @staticmethod
    def _norm_quote(v: Any) -> str:
        return (str(v) if v is not None else "").upper().strip()

    @staticmethod
    def _is_active_status(status: str) -> bool:
        s = str(status or "").strip().lower()
        if not s:
            return True
        banned = ("delist", "suspend", "pause", "settle", "close", "expired")
        return not any(word in s for word in banned)

    def _parse_perp(self, obj: Dict[str, Any], quote: str = "USDT") -> Optional[SymbolInfo]:
        sym = obj.get("symbol")
        if not sym:
            return None

        q = self._norm_quote(obj.get("quoteCurrency") or obj.get("settleCurrency") or "")
        if q != self._norm_quote(quote):
            return None

        sym_s = str(sym).strip()
        if sym_s.startswith("s"):
            return None

        status = str(obj.get("status") or obj.get("state") or obj.get("symbolStatus") or "Listed")
        return SymbolInfo(symbol=sym_s.upper(), status=status, quote=q, raw_data=obj)

    async def get_catalog(self, quote: str = "USDT", only_active: bool = True) -> ProductsCatalog:
        data = await self._get_json("/public/products")
        root = data.get("data") if isinstance(data, dict) else None
        if not isinstance(root, dict):
            return ProductsCatalog(root_data={}, symbols=[])

        arr = root.get("perpProductsV2") or root.get("perpProducts") or []
        out: List[SymbolInfo] = []
        if isinstance(arr, list):
            for it in arr:
                if isinstance(it, dict):
                    si = self._parse_perp(it, quote=quote)
                    if si and (not only_active or self._is_active_status(si.status)):
                        out.append(si)

        if not out:
            for _, value in root.items():
                if isinstance(value, list):
                    for it in value:
                        if isinstance(it, dict):
                            si = self._parse_perp(it, quote=quote)
                            if si and (not only_active or self._is_active_status(si.status)):
                                out.append(si)

        seen = set()
        uniq: List[SymbolInfo] = []
        for symbol in out:
            if symbol.symbol not in seen:
                seen.add(symbol.symbol)
                uniq.append(symbol)
        return ProductsCatalog(root_data=root, symbols=uniq)

    async def get_all(self, quote: str = "USDT", only_active: bool = True) -> List[SymbolInfo]:
        catalog = await self.get_catalog(quote=quote, only_active=only_active)
        return catalog.symbols
