"""HTTP helpers for data-source extraction."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60  # seconds
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.0


def _session(retries: int = MAX_RETRIES, backoff: float = BACKOFF_FACTOR) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_json(
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    """GET request returning parsed JSON."""
    sess = _session()
    logger.info("GET %s params=%s", url, params)
    resp = sess.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def get_text(
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> str:
    """GET request returning raw text."""
    sess = _session()
    logger.info("GET %s params=%s", url, params)
    resp = sess.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def get_csv_stream(
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> requests.Response:
    """GET request returning a streaming response (for CSV/large downloads)."""
    sess = _session()
    logger.info("GET (stream) %s params=%s", url, params)
    resp = sess.get(url, params=params, headers=headers, timeout=timeout, stream=True)
    resp.raise_for_status()
    return resp


def rate_limit_wait(seconds: float = 1.0) -> None:
    """Simple rate-limit sleep between API calls."""
    time.sleep(seconds)
