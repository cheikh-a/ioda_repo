from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .utils import DEFAULT_BASE_URL, append_ndjson, elapsed_ms, ensure_dir, isoformat_utc, utc_now


class IODAError(RuntimeError):
    """Base exception for API/client failures."""


class IODAAPIError(IODAError):
    """Non-retriable API error."""


class IODATransientError(IODAError):
    """Retriable API error (timeouts, 429, 5xx, transport issues)."""


class RateLimiter:
    def __init__(self, min_interval_seconds: float = 0.5) -> None:
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._last_request_monotonic = 0.0

    def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_request_monotonic = time.monotonic()


@dataclass
class APIResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    elapsed_ms: float
    body_bytes: bytes
    json_data: dict[str, Any]

    @property
    def body_text(self) -> str:
        return self.body_bytes.decode("utf-8", errors="replace")

    @property
    def size_bytes(self) -> int:
        return len(self.body_bytes)


class IODAClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        user_agent: str,
        timeout_seconds: float = 60.0,
        min_interval_seconds: float = 0.5,
        max_retries: int = 5,
        request_log_path: Path | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_retries = max(1, int(max_retries))
        self._rate_limiter = RateLimiter(min_interval_seconds=min_interval_seconds)
        self._log_path = request_log_path
        if self._log_path is not None:
            ensure_dir(self._log_path.parent)
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
            },
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "IODAClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _log_request(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        attempt: int,
        status_code: int | None,
        duration_ms: float,
        size_bytes: int | None,
        error: str | None = None,
    ) -> None:
        if self._log_path is None:
            return
        record = {
            "timestamp_utc": isoformat_utc(utc_now()),
            "method": method.upper(),
            "url": url,
            "params": params or {},
            "attempt": attempt,
            "status": status_code,
            "bytes": size_bytes,
            "duration_ms": round(duration_ms, 3),
            "error": error,
        }
        append_ndjson(self._log_path, record)

    def _one_request(self, method: str, path: str, params: dict[str, Any] | None = None) -> APIResponse:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        self._rate_limiter.wait()
        started = time.perf_counter()
        try:
            response = self._client.request(method, url, params=params)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            raise IODATransientError(f"Transport error for {url}: {exc}") from exc
        duration_ms = elapsed_ms(started)
        body = response.content

        if response.status_code in {429, 500, 502, 503, 504}:
            raise IODATransientError(f"HTTP {response.status_code} for {response.url}")
        if response.status_code >= 400:
            raise IODAAPIError(f"HTTP {response.status_code} for {response.url}: {body[:500]!r}")

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise IODATransientError(f"Invalid JSON from {response.url}: {exc}") from exc
        if not isinstance(payload, dict):
            raise IODAAPIError(f"Unexpected JSON root type from {response.url}: {type(payload).__name__}")
        if payload.get("error"):
            raise IODAAPIError(f"IODA API error from {response.url}: {payload.get('error')}")
        return APIResponse(
            url=str(response.url),
            status_code=response.status_code,
            headers={k: v for k, v in response.headers.items()},
            elapsed_ms=duration_ms,
            body_bytes=body,
            json_data=payload,
        )

    def request_json(self, method: str, path: str, params: dict[str, Any] | None = None) -> APIResponse:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        retrying = Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential_jitter(initial=1, max=20),
            retry=retry_if_exception_type(IODATransientError),
            reraise=True,
        )
        last_attempt = 0
        for attempt in retrying:
            with attempt:
                attempt_num = int(attempt.retry_state.attempt_number)
                last_attempt = attempt_num
                try:
                    resp = self._one_request(method, path, params=params)
                except Exception as exc:  # logging before retry/raise
                    self._log_request(
                        method=method,
                        url=url,
                        params=params,
                        attempt=attempt_num,
                        status_code=None,
                        duration_ms=0.0,
                        size_bytes=None,
                        error=str(exc),
                    )
                    raise
                self._log_request(
                    method=method,
                    url=resp.url,
                    params=params,
                    attempt=attempt_num,
                    status_code=resp.status_code,
                    duration_ms=resp.elapsed_ms,
                    size_bytes=resp.size_bytes,
                    error=None,
                )
                return resp
        raise IODAError(f"Request unexpectedly failed without response after {last_attempt} attempts: {url}")

    def get_datasources(self) -> dict[str, Any]:
        return self.request_json("GET", "/datasources/").json_data

    def query_entities(self, **params: Any) -> dict[str, Any]:
        clean = {k: v for k, v in params.items() if v is not None}
        return self.request_json("GET", "/entities/query", params=clean).json_data

    def get_signals_raw(
        self,
        *,
        entity_type: str,
        entity_code: str,
        from_ts: int,
        until_ts: int,
        datasource: str | None = None,
        source_params: str | None = None,
        max_points: int | None = None,
    ) -> APIResponse:
        params: dict[str, Any] = {"from": from_ts, "until": until_ts}
        if datasource:
            params["datasource"] = datasource
        if source_params:
            params["sourceParams"] = source_params
        if max_points is not None:
            params["maxPoints"] = int(max_points)
        return self.request_json("GET", f"/signals/raw/{entity_type}/{entity_code}", params=params)
