"""
varco_fastapi.client.handle
=============================
``JobHandle`` — async handle for background job tracking.

When a client calls an endpoint with ``?with_async=true``, the server returns
``202 Accepted`` with a ``JobAcceptedResponse`` body containing:
- ``job_id``: UUID for the background job
- ``poll_url``: URL to poll for job status
- ``events_url``: optional SSE stream URL for real-time progress

``JobHandle`` wraps the response and provides::

    await job.status()           → JobStatusResponse
    await job.wait()             → result (any) or raises JobFailedError
    await job.cancel()           → bool
    async for ev in job.stream_progress():  → AsyncIterator[JobProgressEvent]

DESIGN: JobHandle over polling loop in client code
    ✅ Encapsulates all job tracking logic — clients get a clean API
    ✅ ``stream_progress()`` falls back to polling if ``events_url`` is None
    ✅ ``wait()`` polls until terminal state — no busy-loop in caller
    ✅ Forwards auth automatically via the owning client
    ❌ Requires the server to expose GET /jobs/{job_id} endpoint

Thread safety:  ✅ Stateless after construction; all state lives in the store.
Async safety:   ✅ All methods are async coroutines.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator
from uuid import UUID

if TYPE_CHECKING:
    from varco_fastapi.client.base import AsyncVarcoClient


# ── JobFailedError ────────────────────────────────────────────────────────────


class JobFailedError(Exception):
    """
    Raised by ``JobHandle.wait()`` when the background job reaches FAILED state.

    Args:
        job_id: The job UUID that failed.
        error:  The error message from the job store.

    Attributes:
        job_id: UUID of the failed job.
        error:  Error string from the server.
    """

    def __init__(self, job_id: UUID, error: str | None) -> None:
        self.job_id = job_id
        self.error = error
        super().__init__(f"Job {job_id} failed: {error or 'unknown error'}")


# ── JobHandle ─────────────────────────────────────────────────────────────────


class JobHandle:
    """
    Async handle for tracking a background job submitted via ``?with_async=true``.

    Returned by client CRUD methods when ``with_async=True`` is passed.  Provides
    polling, cancellation, and optional SSE streaming for real-time progress.

    Args:
        job_id:     UUID of the background job.
        poll_url:   URL to ``GET`` for job status.
        events_url: Optional SSE stream URL for real-time progress events.
                    ``None`` if the server has no event bus configured.
        client:     The owning ``AsyncVarcoClient`` — used for HTTP calls.

    Usage::

        job = await client.create(dto, with_async=True)

        # Option 1: stream progress events
        async for event in job.stream_progress():
            print(f"{event['progress']:.0%} — {event.get('message', '')}")

        # Option 2: poll until done
        result = await job.wait(poll_interval=2.0, timeout=300.0)

        # Option 3: check status
        status = await job.status()
        print(status['status'])

        # Option 4: cancel
        cancelled = await job.cancel()

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ All methods are async.

    Edge cases:
        - ``wait()`` times out → raises ``asyncio.TimeoutError``.
        - ``wait()`` on a FAILED job → raises ``JobFailedError``.
        - ``wait()`` on an already-completed job → returns immediately.
        - ``stream_progress()`` when ``events_url=None`` → falls back to polling.
        - ``cancel()`` for an already-completed job → server returns False / 404.
    """

    def __init__(
        self,
        job_id: UUID,
        poll_url: str,
        events_url: str | None,
        client: AsyncVarcoClient,
    ) -> None:
        self.job_id = job_id
        self.poll_url = poll_url
        self.events_url = events_url
        self._client = client

    async def status(self) -> dict[str, Any]:
        """
        Fetch the current job status.

        Calls ``GET {poll_url}`` via the owning client.

        Returns:
            ``JobStatusResponse`` as a dict (``job_id``, ``status``, ``progress``,
            ``result``, ``error``, ``created_at``, etc.).

        Raises:
            httpx.HTTPStatusError: Server returned non-2xx.
        """
        response = await self._client._request(
            "GET",
            self.poll_url,
            body=None,
            path_params={},
            query_params={},
            response_model=None,  # return raw dict
        )
        # _request returns None for 204; jobs always return 200 with body
        if response is None:
            return {}
        return response  # type: ignore[return-value]

    async def wait(
        self,
        *,
        poll_interval: float = 1.0,
        timeout: float = 120.0,
    ) -> Any:
        """
        Poll until the job reaches a terminal state (COMPLETED, FAILED, CANCELLED).

        Args:
            poll_interval: Seconds between status polls (default 1.0).
            timeout:       Maximum seconds to wait before raising (default 120.0).

        Returns:
            Job result (from ``status['result']``) on COMPLETED.

        Raises:
            asyncio.TimeoutError: Job did not complete within ``timeout`` seconds.
            JobFailedError:       Job reached FAILED state.

        Edge cases:
            - Already completed → returns immediately without polling.
            - CANCELLED state → raises ``JobFailedError`` (no result available).
        """
        _TERMINAL = {"completed", "failed", "cancelled"}

        async def _poll() -> Any:
            while True:
                data = await self.status()
                state = (data.get("status") or "").lower()
                if state == "completed":
                    return data.get("result")
                if state in ("failed", "cancelled"):
                    raise JobFailedError(self.job_id, data.get("error"))
                await asyncio.sleep(poll_interval)

        return await asyncio.wait_for(_poll(), timeout=timeout)

    async def cancel(self) -> bool:
        """
        Request cancellation of the background job.

        Calls ``DELETE {poll_url}`` via the owning client.

        Returns:
            ``True`` if the server accepted the cancellation request.
            ``False`` if the job was not found or already completed.

        Edge cases:
            - 404 → returns ``False`` (job already completed / not found).
            - 409 → returns ``False`` (job already in terminal state).
        """
        try:
            await self._client._request(
                "DELETE",
                self.poll_url,
                body=None,
                path_params={},
                query_params={},
                response_model=None,
                expected_status=204,
            )
            return True
        except Exception:  # noqa: BLE001 — any failure means cancel did not succeed
            return False

    async def stream_progress(self) -> AsyncIterator[dict[str, Any]]:
        """
        Stream real-time progress events for this job.

        If ``events_url`` is set, connects to the SSE endpoint and yields
        ``JobProgressEvent`` dicts as they arrive.  The stream ends automatically
        when the job reaches a terminal state (COMPLETED, FAILED, CANCELLED).

        Falls back to polling every 1 second if ``events_url`` is None.

        Yields:
            Dicts representing ``JobProgressEvent`` (``job_id``, ``status``,
            ``progress``, ``message``, ``error``).

        Edge cases:
            - ``events_url=None`` → falls back to polling (1s interval).
            - SSE connection drops → raises immediately (caller must retry).
            - Job already completed before streaming starts → yields terminal event.
        """
        if self.events_url is None:
            # Fall back to polling — yield synthetic progress events
            async for event in self._poll_for_progress():
                yield event
            return

        # SSE streaming via httpx
        # httpx supports streaming responses — we read SSE line-by-line
        async for event in self._stream_sse():
            yield event

    async def _poll_for_progress(self) -> AsyncIterator[dict[str, Any]]:
        """
        Fallback: poll the status endpoint and yield synthetic progress events.

        Yields:
            Status dicts until the job reaches a terminal state.
        """
        _TERMINAL = {"completed", "failed", "cancelled"}
        while True:
            data = await self.status()
            yield data
            state = (data.get("status") or "").lower()
            if state in _TERMINAL:
                return
            await asyncio.sleep(1.0)

    async def _stream_sse(self) -> AsyncIterator[dict[str, Any]]:
        """
        Stream Server-Sent Events from ``events_url``.

        Uses ``httpx.AsyncClient`` streaming to parse SSE ``data:`` lines.

        Yields:
            Parsed ``JobProgressEvent`` dicts.

        Edge cases:
            - Non-JSON ``data:`` lines → skipped silently.
            - Empty data lines → skipped.
            - Connection closed by server → stops iteration.
        """
        import json as _json  # noqa: PLC0415

        _TERMINAL = {"completed", "failed", "cancelled"}

        client = self._client._client
        if client is None:
            # Not in context manager — create temporary client for SSE
            client = self._client._build_httpx_client()
            owns_client = True
        else:
            owns_client = False

        try:
            async with client.stream("GET", self.events_url) as response:
                async for line in response.aiter_lines():
                    line = line.strip()
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw:
                        continue
                    try:
                        event = _json.loads(raw)
                    except _json.JSONDecodeError:
                        continue
                    yield event
                    state = (event.get("status") or "").lower()
                    if state in _TERMINAL:
                        return
        finally:
            if owns_client:
                await client.aclose()

    def __repr__(self) -> str:
        """Return a concise string representation for debugging."""
        return f"JobHandle(" f"job_id={self.job_id}, " f"poll_url={self.poll_url!r})"


__all__ = [
    "JobHandle",
    "JobFailedError",
]
