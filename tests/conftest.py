"""Pytest configuration: ensure test logs print inputs and decision bases."""
from __future__ import annotations

import logging
import sys


def _configure_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    logger = logging.getLogger("observability.tests")
    logger.setLevel(logging.INFO)
    logger.handlers[:] = [handler]
    logger.propagate = False
    return logger


_configure_logging()
