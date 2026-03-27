"""
varco_core.observability.metric
================================
``Metric`` — ergonomic wrapper around a single OTel instrument for custom
named metrics with kwargs-as-attributes API.

Use this when you want to track a business-level quantity (token usage,
tool calls, audit records created, active sessions, etc.) that doesn't
map neatly to a fixed decorator call site.

Public API
----------
``Metric(name, kind, ...)``
    Wraps a single OTel instrument.  Call ``.add()`` to increment,
    ``.sub()`` to decrement, ``.record()`` to record an arbitrary value.

``register_gauge(name, callback, ...)``
    Registers an OTel ``ObservableGauge`` driven by a user-supplied
    callback function.  Use this for values you *sample* (CPU %, memory
    used, config flag) rather than values you *accumulate*.

Usage — counter (monotone, only increases)::

    from varco_core.observability.metric import Metric

    _tool_calls = Metric("agent.tool_calls", kind="counter",
                         description="Number of tool invocations")

    async def invoke_tool(name: str) -> Any:
        _tool_calls.add(tool=name)           # kwargs → OTel attributes
        return await _tools[name].run()

Usage — up-down counter (can increase and decrease)::

    _active_sessions = Metric(
        "sessions.active",
        kind="updown_counter",
        description="Currently active user sessions",
    )

    def on_connect(user_id: str) -> None:
        _active_sessions.add(user=user_id)   # +1 (default)

    def on_disconnect(user_id: str) -> None:
        _active_sessions.sub(user=user_id)   # -1

Usage — histogram (distribution, e.g. token counts per call)::

    _tokens = Metric(
        "llm.tokens",
        kind="histogram",
        unit="1",
        description="Tokens consumed per LLM call",
    )

    async def call_llm(prompt: str) -> str:
        response = await _client.generate(prompt)
        _tokens.record(response.usage.total_tokens, model=response.model)
        return response.text

Usage — observable gauge (pull-based, callback-driven)::

    from varco_core.observability.metric import register_gauge

    register_gauge(
        "memory.heap_used",
        callback=lambda: process.memory_info().rss,
        unit="By",
        description="JVM-style heap metric via psutil",
    )

DESIGN: Metric wraps a single OTel instrument, not one-per-attribute-set
    ✅ One object per logical metric — easy to pass around, mock in tests.
    ✅ kwargs-as-attributes API is more readable than positional dict args.
    ✅ Lazy instrument creation avoids the "provider not yet configured" race.
    ❌ A thin wrapper — not type-safe on attribute names (unlike a DTO).
       Attribute typos surface at runtime in the observability backend,
       not at coding time.  Acceptable for observability code.

DESIGN: sub() as a first-class method instead of just add(-amount)
    ✅ Makes intent explicit for updown_counter use cases (sessions, jobs).
    ✅ Matches mental model of "add to pool / remove from pool".
    ❌ counter kind does not support negative values — sub() will still
       call add(-amount); the OTel SDK will emit a warning or drop the
       point.  Document clearly; callers must use updown_counter for
       decrements.

Thread safety:  ✅ _get_instrument() is idempotent under GIL (CPython).
                   The underlying OTel instrument is thread-safe.
Async safety:   ✅ add/sub/record are synchronous calls — safe from any
                   coroutine; no await required.
"""

from __future__ import annotations

from typing import Any, Literal

from opentelemetry import metrics as otel_metrics

# Reuse the shared lazy-cache from metrics.py — same key format (meter_name, name).
# This guarantees that Metric("orders.created") and create_counter("orders.created")
# with the same meter_name return the same OTel instrument object.
from varco_core.observability.metrics import _instrument_cache

# ── Type alias ───────────────────────────────────────────────────────────────

# The three synchronous push-based OTel instrument kinds.
# "counter"         — monotone, only .add() with non-negative values
# "updown_counter"  — bidirectional, .add() and .sub() (negative add) both valid
# "histogram"       — distribution, .record() any float value
MetricKind: type = Literal["counter", "updown_counter", "histogram"]


# ── Metric ───────────────────────────────────────────────────────────────────


class Metric:
    """
    Ergonomic wrapper around a single named OTel instrument.

    Unlike ``@counter`` / ``@histogram`` decorators (which are tied to a
    single function call site), ``Metric`` lets you record observations
    from multiple call sites, with dynamic attribute values supplied as
    keyword arguments.

    The underlying OTel instrument is created lazily on the first
    ``add()`` / ``sub()`` / ``record()`` call, so it is safe to
    instantiate ``Metric`` at module level before the ``MeterProvider``
    is configured (e.g. in global-scope initializers).

    Args:
        name:
            OTel metric name following naming conventions
            (e.g. ``"agent.tool_calls"``, ``"llm.tokens_used"``).
        kind:
            Instrument kind — ``"counter"``, ``"updown_counter"``, or
            ``"histogram"``.  Pick ``"counter"`` for monotone counts,
            ``"updown_counter"`` for values that can decrease (active
            connections, queue depth), ``"histogram"`` for distributions
            (latency, payload size, token counts).
        meter_name:
            OTel meter / instrumentation scope name.  Defaults to
            ``"varco"``.  Override per service if you want to filter
            metrics by scope in your observability backend.
        description:
            Human-readable description shown in metric explorer UIs.
        unit:
            OTel unit string per semantic conventions (``"1"`` for
            dimensionless counts, ``"s"`` for seconds, ``"By"`` for
            bytes, ``"ms"`` for milliseconds).

    Edge cases:
        - ``sub()`` on a ``"counter"`` kind will call ``add(-amount)``
          — OTel SDK may drop or warn on negative counter increments.
          Always use ``"updown_counter"`` when decrements are needed.
        - ``record()`` on a ``"counter"`` or ``"updown_counter"`` is an
          alias for ``add()`` — both route to ``.add()`` on the
          underlying instrument.  For histograms, ``record()`` calls
          ``.record()``.
        - If no ``MeterProvider`` is active, OTel returns a no-op meter
          and the instrument is a no-op — calls succeed silently.

    Thread safety:  ✅ Instrument creation is idempotent under CPython GIL.
    Async safety:   ✅ All methods are synchronous — safe to call from
                       any coroutine without ``await``.

    Example::

        # Module-level — safe before DI configures MeterProvider
        _audit_created = Metric(
            "audit.records_created",
            kind="counter",
            description="Audit log entries persisted",
        )

        async def create_audit(entry: AuditEntry) -> None:
            await _repo.save(entry)
            _audit_created.add(entity=entry.entity_type, tenant=entry.tenant_id)
    """

    # __slots__ keeps per-instance memory small — no __dict__ allocated.
    # All attributes are stored in fixed slots, which also prevents accidental
    # attribute addition (e.g. typo _naem = ... instead of _name).
    __slots__ = ("_name", "_kind", "_meter_name", "_description", "_unit")

    def __init__(
        self,
        name: str,
        kind: MetricKind = "counter",
        *,
        meter_name: str = "varco",
        description: str = "",
        unit: str = "1",
    ) -> None:
        # Store config — instrument is created lazily on first use so that
        # module-level Metric() calls don't race against MeterProvider setup.
        self._name = name
        self._kind = kind
        self._meter_name = meter_name
        self._description = description
        self._unit = unit

    # ── Public API ────────────────────────────────────────────────────────────

    def add(self, amount: int | float = 1, /, **attributes: str) -> None:
        """
        Increment the metric by ``amount``.

        Keyword arguments are forwarded as OTel attribute key-value pairs,
        which appear as label dimensions in your metrics backend.

        Args:
            amount:
                The value to add (default ``1``).  Must be non-negative
                for ``"counter"`` kind; any value is valid for
                ``"updown_counter"`` and ``"histogram"``.
            **attributes:
                Arbitrary string key-value pairs attached to this
                observation.  Use these for dimensions like
                ``tool="search"``, ``model="gpt-4"``, ``tenant="acme"``.

        Example::

            _tool_calls.add(tool="web_search", agent="planner")
            _token_budget.add(100, model="gpt-4o", tenant="acme")
        """
        instrument = self._get_instrument()
        if self._kind == "histogram":
            # Histogram uses .record() — semantics differ from counter .add()
            instrument.record(amount, attributes=attributes or None)
        else:
            # counter and updown_counter both use .add()
            instrument.add(amount, attributes=attributes or None)

    def sub(self, amount: int | float = 1, /, **attributes: str) -> None:
        """
        Decrement the metric by ``amount`` — convenience alias for
        ``add(-amount)``.

        Intended for ``"updown_counter"`` use cases such as active
        sessions, in-flight requests, or queue depth.

        Args:
            amount:
                The positive value to subtract (default ``1``).  The
                underlying OTel call receives ``-amount``.
            **attributes:
                Same label dimensions as ``add()``.

        Warning:
            Calling ``sub()`` on a ``"counter"`` kind metric will pass a
            negative increment to the OTel SDK, which may drop the data
            point or log a warning.  Use ``"updown_counter"`` instead.

        Example::

            _active_sessions.add(user=user_id)    # login
            _active_sessions.sub(user=user_id)    # logout
        """
        # Negate amount so the caller writes sub(1) instead of add(-1)
        self.add(-amount, **attributes)

    def record(self, value: float, /, **attributes: str) -> None:
        """
        Record an arbitrary value — semantic alias for ``add()``.

        More readable than ``add()`` when the metric represents a
        distribution (histogram) rather than a monotone count.  Both
        methods are interchangeable.

        Args:
            value:
                The observation to record.
            **attributes:
                Label dimensions — same as ``add()``.

        Example::

            _tokens_used.record(response.usage.total_tokens, model=model_id)
            _payload_bytes.record(len(payload), channel=channel_name)
        """
        self.add(value, **attributes)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_instrument(self) -> Any:
        """
        Return the cached OTel instrument, creating it on first call.

        Uses the same ``_instrument_cache`` dict as ``create_counter`` /
        ``create_histogram`` so that a ``Metric`` and an imperative helper
        targeting the same ``(meter_name, name)`` share a single instrument
        instance — avoids duplicate registration warnings from the OTel SDK.

        Returns:
            The underlying ``Counter``, ``UpDownCounter``, or ``Histogram``
            OTel instrument.

        Thread safety:  ✅ dict writes are atomic under CPython GIL.
        """
        key = (self._meter_name, self._name)
        instrument = _instrument_cache.get(key)
        if instrument is None:
            meter = otel_metrics.get_meter(self._meter_name)
            instrument = self._create_instrument(meter)
            _instrument_cache[key] = instrument
        return instrument

    def _create_instrument(self, meter: Any) -> Any:
        """
        Delegate to the correct meter factory method based on ``_kind``.

        Args:
            meter: An OTel ``Meter`` instance from ``otel_metrics.get_meter()``.

        Returns:
            The newly created OTel instrument.

        Raises:
            ValueError: If ``_kind`` is not one of the three supported values.
                        This should be unreachable if ``MetricKind`` is used
                        correctly — but we fail loudly rather than silently
                        returning a no-op.

        Edge cases:
            - No active ``MeterProvider`` → OTel returns a no-op meter;
              this method still returns a no-op instrument without error.
        """
        if self._kind == "counter":
            return meter.create_counter(
                name=self._name,
                description=self._description,
                unit=self._unit,
            )
        if self._kind == "updown_counter":
            return meter.create_up_down_counter(
                name=self._name,
                description=self._description,
                unit=self._unit,
            )
        if self._kind == "histogram":
            return meter.create_histogram(
                name=self._name,
                description=self._description,
                unit=self._unit,
            )
        # Guard against invalid kind values at runtime — explicit is better
        # than triggering an AttributeError on the no-op instrument later.
        raise ValueError(
            f"Metric kind {self._kind!r} is not supported. "
            f"Expected one of: 'counter', 'updown_counter', 'histogram'. "
            f"Use register_gauge() for observable (pull-based) gauges."
        )

    def __repr__(self) -> str:
        return (
            f"Metric("
            f"name={self._name!r}, "
            f"kind={self._kind!r}, "
            f"meter={self._meter_name!r})"
        )


# ── register_gauge ─────────────────────────────────────────────────────────


def register_gauge(
    name: str,
    callback: Any,
    *,
    meter_name: str = "varco",
    description: str = "",
    unit: str = "1",
    static_attributes: dict[str, str] | None = None,
) -> None:
    """
    Register a pull-based ``ObservableGauge`` driven by ``callback``.

    Unlike ``Metric`` (push-based — you call ``.add()`` explicitly),
    ``ObservableGauge`` is *pull-based*: the OTel SDK calls your
    ``callback`` on each metric collection cycle and records the
    returned value as the current gauge observation.

    Use this for values that are cheaply sampled on demand:

    - System metrics (CPU %, heap used, file descriptor count)
    - External pool sizes (DB connections available, thread pool depth)
    - Configuration snapshots (feature flag states, rate-limit thresholds)

    For values you increment/decrement explicitly, use ``Metric`` with
    ``kind="updown_counter"`` instead — it is more efficient and composes
    better with attribute filtering.

    Args:
        name:
            Metric name (e.g. ``"process.memory_rss"``).
        callback:
            Callable with signature ``() -> int | float`` — invoked by
            the SDK on each collection cycle.  Must be fast and
            non-blocking; slow callbacks delay metric export.
        meter_name:
            OTel meter scope name.  Defaults to ``"varco"``.
        description:
            Human-readable description.
        unit:
            OTel unit string (``"By"`` for bytes, ``"%"`` for
            percentage, ``"1"`` for dimensionless).
        static_attributes:
            Fixed attribute dict attached to every observation reported
            by this gauge.  Dynamic attributes are not supported for
            observable gauges — use multiple ``register_gauge()`` calls
            with different ``static_attributes`` if needed.

    Edge cases:
        - If no ``MeterProvider`` is active, the registration succeeds
          but the callback is never invoked.
        - ``callback`` must not raise — exceptions inside an observable
          callback are caught by the OTel SDK and the data point is
          dropped silently.
        - Calling ``register_gauge`` with the same ``name`` and
          ``meter_name`` twice creates two gauges on the same meter,
          which may produce duplicate observations.  Guard at the call
          site if needed.

    Thread safety:  ✅ Registration is a one-shot call at startup.
    Async safety:   ✅ Callback is synchronous — do not use async
                       functions as the callback.

    Example::

        import psutil

        register_gauge(
            "process.memory_rss",
            callback=lambda: psutil.Process().memory_info().rss,
            unit="By",
            description="Resident set size of this process",
            static_attributes={"pod": os.environ.get("POD_NAME", "unknown")},
        )
    """
    meter = otel_metrics.get_meter(meter_name)

    # Wrap the user's zero-arg callback to produce the sequence of
    # ``Observation`` objects the OTel SDK expects.  Static attributes are
    # merged here so the caller doesn't need to know OTel internals.
    from opentelemetry.metrics import Observation

    attrs = static_attributes or {}

    def _otel_callback(options: Any) -> list[Observation]:  # noqa: ARG001
        # options is an ObservableCallbackOptions — not used here but
        # required by the OTel SDK callback signature.
        value = callback()
        return [Observation(value, attributes=attrs)]

    meter.create_observable_gauge(
        name=name,
        callbacks=[_otel_callback],
        description=description,
        unit=unit,
    )


__all__ = [
    "Metric",
    "MetricKind",
    "register_gauge",
]
