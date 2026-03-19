from __future__ import annotations

import inspect
import logging
import os
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

import pytz

from consts import LOG_DEBUG, LOG_ERROR, LOG_INFO, LOG_WARNING, MAX_LOG_LINES, TIME_ZONE


TZ = pytz.timezone(TIME_ZONE)


class _TzFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


class UnifiedLogger:
    def __init__(self, name: str, log_dir: str = "./logs", max_lines: int = MAX_LOG_LINES, context: Optional[str] = None):
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_path = Path(log_dir) / f"{name}.log"
        approx_line_len = 350
        max_bytes = max(100_000, approx_line_len * max_lines)

        base_logger = logging.getLogger(name)
        base_logger.setLevel(logging.DEBUG)
        base_logger.propagate = False

        if not base_logger.handlers:
            handler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=2, encoding="utf-8")
            handler.setFormatter(_TzFormatter("%(asctime)s | %(levelname)s | %(context)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
            base_logger.addHandler(handler)

        self._logger = logging.LoggerAdapter(base_logger, extra={"context": context or name})

    def debug(self, msg: str, *args, **kwargs) -> None:
        if LOG_DEBUG:
            print(msg)
            self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        if LOG_INFO:
            print(msg)
            self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        if LOG_WARNING:
            print(msg)
            self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        if LOG_ERROR:
            print(msg)
            self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        if LOG_ERROR:
            print(msg)
            self._logger.exception(msg, *args, **kwargs)

    def total_exception_decor(self, func, context: Optional[Any] = None):
        if getattr(func, "_is_wrapped", False):
            return func

        if context is not None:
            target_logger = logging.LoggerAdapter(self._logger.logger, extra={"context": context})
        else:
            target_logger = self._logger

        if hasattr(func, "__call__"):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception:
                    target_logger.exception("Unhandled async exception in %s", getattr(func, "__qualname__", repr(func)))
                    return None

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    target_logger.exception("Unhandled sync exception in %s", getattr(func, "__qualname__", repr(func)))
                    return None

            wrapper = async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
            wrapper._is_wrapped = True
            return wrapper
        return func
