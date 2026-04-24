# Coding Standard

This document defines the coding standards for all packages in this repository.
It applies to every contributor and to all AI-assisted code generation.

> **Formatting is automated.** `ruff` and `black` run in pre-commit and enforce
> all whitespace, line-length, import ordering, and style rules automatically.
> Do **not** configure or argue about formatting manually — let the tools win.

---

## Table of Contents

1. [Language Basics](#1-language-basics)
2. [Naming Conventions](#2-naming-conventions)
3. [Type Annotations](#3-type-annotations)
4. [Docstrings](#4-docstrings)
5. [Exceptions](#5-exceptions)
6. [Imports](#6-imports)
7. [Module Structure](#7-module-structure)
8. [Classes](#8-classes)
9. [Functions and Methods](#9-functions-and-methods)
10. [Abstractions and Reuse](#10-abstractions-and-reuse)
11. [Concurrency and Async](#11-concurrency-and-async)
12. [Testing — Unit Tests](#12-testing--unit-tests)
13. [Testing — Integration Tests](#13-testing--integration-tests)
14. [Testing — Broken Tests Policy](#14-testing--broken-tests-policy)
15. [Design Decisions](#15-design-decisions)
16. [What Not to Do](#16-what-not-to-do)

---

## 1. Language Basics

### Python version
All code targets **Python 3.12+**.

### Future annotations
Every `.py` file must begin with:

```python
from __future__ import annotations
```

This enables PEP 604 union syntax (`X | Y`), forward references in annotations,
and prevents annotation evaluation at import time — required for `TYPE_CHECKING`
guards to work correctly.

### Constants and sentinels
Module-level constants are `UPPER_SNAKE_CASE`.
Use `typing.Final` for values that must never be rebound:

```python
from typing import Final

CHANNEL_ALL: Final = "*"
DEFAULT_TIMEOUT_SECONDS: Final = 30.0
```

### String formatting
Prefer f-strings over `.format()` or `%`-style formatting in all new code.

---

## 2. Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Module | `lower_snake_case` | `circuit_breaker.py` |
| Package | `lower_snake_case` | `varco_core/` |
| Class | `UpperCamelCase` | `CircuitBreaker` |
| Abstract class / Protocol | `Abstract` or `I` prefix, or descriptive noun | `AbstractEventBus`, `IUoWProvider` |
| Exception | `UpperCamelCase` ending in `Error` | `CircuitOpenError` |
| Function / method | `lower_snake_case` | `compute_delay()` |
| Private method / attribute | `_leading_underscore` | `_failure_count` |
| Dunder / magic | `__double_underscore__` | `__post_init__` |
| Type variable | Short `UpperCamelCase` or single letter | `_R`, `D`, `PK` |
| Constant | `UPPER_SNAKE_CASE` | `MAX_RETRY_ATTEMPTS` |
| Enum member | `UPPER_SNAKE_CASE` | `CircuitState.HALF_OPEN` |

**Abbreviations**: avoid inventing new abbreviations. Spell things out
(`configuration`, not `cfg`; `repository`, not `repo`) unless the abbreviated
form is the established name in the domain or the existing codebase uses it
consistently.

---

## 3. Type Annotations

### Annotate everything
Every function parameter, return type, and class attribute must carry a type
annotation. No `Any` unless it genuinely is unknown and documenting why is
impractical.

```python
# ✅ Correct
def compute_delay(attempt: int, policy: RetryPolicy) -> float:
    ...

# ❌ Wrong — unannotated parameters
def compute_delay(attempt, policy):
    ...
```

### Use standard library types from `collections.abc`
Import abstract types from `collections.abc`, not `typing`:

```python
from collections.abc import Callable, Awaitable, Sequence, Mapping

# ✅
def register(handler: Callable[[Event], Awaitable[None]]) -> None: ...

# ❌ — deprecated aliases
from typing import Callable, List
```

### Use `X | Y` union syntax
Prefer PEP 604 union syntax over `Optional[X]` or `Union[X, Y]`:

```python
# ✅
def get(key: str) -> str | None: ...

# ❌
from typing import Optional, Union
def get(key: str) -> Optional[str]: ...
```

### `TYPE_CHECKING` guards for circular imports
If a type hint would create a circular import at runtime, guard it:

```python
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.event.dlq import AbstractDeadLetterQueue
```

The annotation is still valid as a string thanks to `from __future__ import annotations`.

### `TypeVar` placement
Declare `TypeVar`s at module level, prefixed with `_`:

```python
from typing import TypeVar

_T = TypeVar("_T")
_D = TypeVar("_D", bound="DomainModel")
```

### `runtime_checkable` Protocols
If a Protocol is used with `isinstance()` checks, decorate it with
`@runtime_checkable`. Otherwise omit it — it adds runtime overhead.

---

## 4. Docstrings

Every public module, class, method, and function must have a docstring.
Private helpers (`_leading_underscore`) must have a docstring if their purpose
is not immediately obvious from the name and implementation.

### Format: Google style

Use **Google-style docstrings** throughout. Black and ruff do not reformat
docstring content, so maintain consistent indentation manually.

#### Module docstring

```python
"""
varco_core.resilience.circuit_breaker
======================================
One-line summary of what this module provides.

Longer description of the module's purpose, design rationale, and any
non-obvious usage constraints.

Usage::

    from varco_core.resilience import CircuitBreaker, CircuitBreakerConfig

    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=5))
    result = await breaker.call_async(my_coroutine, arg1, arg2)

DESIGN: <short title>
    ✅ Benefit 1
    ✅ Benefit 2
    ❌ Drawback / trade-off

Thread safety:  ✅ / ⚠️ / ❌ — one-line statement
Async safety:   ✅ / ⚠️ / ❌ — one-line statement
"""
```

#### Class docstring

```python
class CircuitBreaker:
    """
    One-line summary.

    Longer explanation of what the class does, its invariants, and
    any constraints on construction or lifecycle.

    Attributes:
        state: Current ``CircuitState``. Transitions are managed internally.
        config: Immutable ``CircuitBreakerConfig`` supplied at construction.

    Example::

        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))
        result = await breaker.call_async(fetch_user, user_id)

    Thread safety:  ⚠️  Safe within a single asyncio event loop.
    Async safety:   ✅  ``asyncio.Lock`` serialises HALF_OPEN state transitions.
    """
```

#### Method / function docstring

```python
async def call_async(
    self,
    fn: Callable[..., Awaitable[_R]],
    *args: object,
    **kwargs: object,
) -> _R:
    """
    Call ``fn`` through the circuit breaker.

    Checks the current circuit state before invoking ``fn``.  Records
    success or failure and transitions state accordingly.

    Args:
        fn:     Async callable to protect.
        *args:  Positional arguments forwarded to ``fn``.
        **kwargs: Keyword arguments forwarded to ``fn``.

    Returns:
        The value returned by ``fn`` on success.

    Raises:
        CircuitOpenError: The circuit is OPEN or a concurrent HALF_OPEN
            probe is already in progress.
        Exception: Any exception raised by ``fn`` (recorded as a failure
            unless not in ``config.monitored_on``).

    Edge cases:
        - If ``fn`` raises an exception not in ``config.monitored_on``,
          the failure is NOT counted against the circuit — the exception
          propagates immediately without changing state.
        - Concurrent calls during HALF_OPEN are rejected fast; only the
          first caller probes.
    """
```

#### When to include an `Example` section
Include a code example in the docstring when:
- The API has multiple calling conventions or optional parameters with
  non-obvious interactions.
- The function has important usage constraints (e.g., "must be a shared
  instance", "call `start()` before use").
- The function is regularly used by contributors who are not its author.

#### `DESIGN:` blocks
Significant non-obvious design decisions must be documented with a `DESIGN:`
block. Place it in the module docstring or in the class docstring where the
decision was made:

```
DESIGN: shared CircuitBreaker instance vs. per-call state
    ✅ Shared instance correctly counts failures from ALL callers
    ✅ State transitions are centralised — no split-brain
    ❌ Requires asyncio.Lock for HALF_OPEN — adds slight latency
```

---

## 5. Exceptions

### Use specific exception types
Never raise a bare `Exception`, `RuntimeError`, or `ValueError` unless the
standard library or an explicit convention demands it. Define a domain-specific
exception class instead.

```python
# ✅ Correct — specific, descriptive
class CircuitOpenError(Exception):
    """Raised when a call is rejected because the circuit is OPEN."""

    def __init__(self, state: CircuitState) -> None:
        super().__init__(f"Circuit is {state.value}; call rejected.")
        self.state = state


# ❌ Wrong — generic, loses structured context
raise RuntimeError("Circuit is open")
```

### Exception hierarchy
Group related exceptions under a common base. This lets callers catch
either the base (broad) or a subclass (precise):

```python
class VarcoError(Exception):
    """Base class for all varco errors."""

class RepositoryError(VarcoError):
    """Base class for repository-layer errors."""

class EntityNotFoundError(RepositoryError):
    """Raised when a requested entity does not exist."""

    def __init__(self, entity_type: type, pk: object) -> None:
        super().__init__(f"{entity_type.__name__} with pk={pk!r} not found.")
        self.entity_type = entity_type
        self.pk = pk
```

### `__post_init__` validation
For frozen dataclasses used as configuration objects, raise `ValueError` in
`__post_init__` only for genuine invariant violations (not domain exceptions).
`ValueError` is acceptable here because it signals a programming error at
object construction time, analogous to `TypeError`.

```python
def __post_init__(self) -> None:
    if self.failure_threshold < 1:
        raise ValueError(
            f"failure_threshold must be ≥ 1, got {self.failure_threshold}."
        )
```

### `except` clauses
Catch the most specific exception you can handle. Never `except Exception` or
`except BaseException` unless you are writing infrastructure-level code
(e.g., a retry wrapper, a DLQ push) that genuinely needs to handle anything.
When you do catch broadly, log and re-raise or document why swallowing is safe:

```python
# ✅ DLQ push must never raise — documented, intentional swallow
try:
    await self._dlq.push(event, error)
except Exception:
    _logger.exception("DLQ push failed; event will be lost: %r", event)
    # Swallowed intentionally — retry wrapper cannot recover from DLQ failure
```

---

## 6. Imports

### Ordering (enforced by ruff/isort)
1. `from __future__ import annotations` — always first
2. Standard library
3. Third-party packages
4. Local packages (`varco_*`)
5. Relative imports within the same package

### Absolute imports
Prefer absolute imports everywhere. Use relative imports only when the
module is tightly coupled to its package and a relative import avoids a
long repetitive prefix:

```python
# ✅ Absolute
from varco_core.event.base import AbstractEventBus

# ✅ Relative — acceptable for intra-package siblings
from .base import AbstractEventBus
```

### No wildcard imports
Never use `from module import *`. Always import names explicitly so static
analysis tools can trace them.

### `__all__`
Public packages must define `__all__` in their `__init__.py` to declare their
public surface. This prevents accidental re-exports and clarifies the intended API.

---

## 7. Module Structure

Follow this top-to-bottom order within every module:

```
1.  Module docstring
2.  from __future__ import annotations
3.  Standard library imports
4.  Third-party imports
5.  Local / intra-package imports
6.  TYPE_CHECKING block (if needed)
7.  Module-level logger:  _logger = logging.getLogger(__name__)
8.  Module-level constants and TypeVars
9.  ── Section header comment  ──────────────────────────────────
10. Class / function definitions (logical grouping, not alphabetical)
```

Use ASCII section headers to separate logical groups in large files:

```python
# ── CircuitBreakerConfig ──────────────────────────────────────────────────────
```

---

## 8. Classes

### Frozen dataclasses for value objects and config
All value objects, configuration objects, and AST nodes must be
`@dataclass(frozen=True)`. Mutable dataclasses are a red flag and require
explicit justification:

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 1.0
```

### `ABC` for abstract base classes
All abstract classes must inherit from `abc.ABC` and mark abstract methods
with `@abstractmethod`. Do not use `raise NotImplementedError` as a substitute
for `@abstractmethod` — it does not prevent instantiation.

```python
from abc import ABC, abstractmethod

class AbstractEventBus(ABC):
    @abstractmethod
    async def publish(self, event: Event, channel: str) -> None: ...
```

### `Protocol` for structural typing
Prefer `Protocol` over `ABC` when the interface is meant to be satisfied
structurally (without explicit inheritance), especially for small, focused
interfaces like `AsyncCache`:

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class AsyncCache(Protocol[_K, _V]):
    async def get(self, key: _K) -> _V | None: ...
    async def set(self, key: _K, value: _V) -> None: ...
```

### `__slots__`
Use `__slots__` for classes that are instantiated very frequently (e.g., event
objects, AST nodes). Dataclasses automatically benefit from `__slots__=True`
in Python 3.10+.

### `Enum` for fixed sets of values
Use `Enum` (or `StrEnum`) for any fixed set of named values. Never use bare
string constants when an enum provides correctness guarantees:

```python
class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
```

---

## 9. Functions and Methods

### One responsibility per function
A function does one thing. If you find yourself writing `# step 1 … step 2 …`
in the function body, extract helper functions.

### Keep functions short
Functions longer than ~40 lines are a signal to refactor. There is no hard
limit, but long functions should carry a comment explaining why they cannot
be split.

### Avoid boolean flag parameters
Boolean flags that change a function's behaviour are a code smell. Split into
two functions or use an enum:

```python
# ❌
def fetch(include_deleted: bool = False) -> list[User]: ...

# ✅
def fetch_active() -> list[User]: ...
def fetch_including_deleted() -> list[User]: ...
```

### Default argument values
Never use mutable defaults. Use `None` as the sentinel and create the mutable
object inside the function:

```python
# ❌
def process(items: list[str] = []) -> None: ...

# ✅
def process(items: list[str] | None = None) -> None:
    if items is None:
        items = []
```

For frozen dataclasses, use `field(default_factory=...)`:

```python
monitored_on: tuple[type[Exception], ...] = field(
    default_factory=lambda: (Exception,)
)
```

### Return types
Never return `None` implicitly from a function that is expected to return a
value. All code paths must return the declared type.

---

## 10. Abstractions and Reuse

### Abstract before you implement twice
If you write the same logic in two places, extract it. If a new feature
introduces a concept that other components might need to plug into, define
a Protocol or ABC in `varco_core` first, then provide a concrete
implementation in the relevant backend package.

```
New concept → Protocol/ABC in varco_core  →  Concrete impl in varco_[backend]
```

This is the established pattern for `AbstractEventBus`, `AsyncCache`,
`AbstractRepository`, `AbstractDeadLetterQueue`, and every other extension
point in the codebase.

### Don't abstract prematurely
A utility function used in exactly one place is not a reusable utility — it is
just an extracted helper. Add the abstraction layer when there is a second
concrete need, not in anticipation of one.

### Mixin composition via MRO
When adding cross-cutting behaviour to services (caching, tenant isolation,
soft delete, validation), use Python's MRO-based mixin pattern. Every hook
method must call `super()`:

```python
class TenantAwareService(AsyncService):
    async def _scoped_params(self, params: QueryParams) -> QueryParams:
        scoped = await super()._scoped_params(params)
        return scoped.filter_by_tenant(self._tenant_id)
```

Forgetting `super()` silently drops all mixins that follow in the MRO.

### DI wiring is the only place that knows concrete types
Application code (services, handlers) must depend on abstractions
(Protocols/ABCs), never on concrete implementations. Only `@Configuration`
classes in `di.py` files may reference concrete types:

```python
# ✅ Service depends on abstract interface
class OrderService:
    def __init__(self, producer: AbstractEventProducer) -> None: ...

# ❌ Service knows about Kafka — breaks swappability and testability
class OrderService:
    def __init__(self, producer: KafkaEventProducer) -> None: ...
```

---

## 11. Concurrency and Async

### `asyncio.Lock` must be created lazily
Locks created at module level or in `__init__` raise `RuntimeError: no running
event loop` in Python 3.10+. Always create them on first use inside a method:

```python
# ❌
class MyService:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()  # No event loop yet!

# ✅
class MyService:
    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None

    async def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
```

### Always `await` async calls
Never store a coroutine in a variable without immediately awaiting it. Unawaited
coroutines are silently discarded and their side effects never run. Enable
`asyncio` debug mode in development to catch leaks:

```bash
PYTHONASYNCIODEBUG=1 uv run pytest ...
```

### Background tasks
Use `asyncio.create_task()` for fire-and-forget background work. Always assign
the returned `Task` object to a variable or a set — otherwise the task can be
garbage-collected before it completes:

```python
# ✅
self._tasks: set[asyncio.Task] = set()

task = asyncio.create_task(self._poll_loop())
self._tasks.add(task)
task.add_done_callback(self._tasks.discard)
```

### Shared resilience instances
`CircuitBreaker` and `Bulkhead` must be shared instances per external
dependency. A per-call instance never accumulates enough failures/concurrency
to trip. Declare them as instance attributes in `__init__` (not per-call):

```python
class PaymentGateway:
    def __init__(self) -> None:
        self._breaker = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60.0)
        )
        self._bulkhead = Bulkhead(BulkheadConfig(max_concurrent=10))
```

---

## 12. Testing — Unit Tests

### All tests are `async def`
The workspace uses `asyncio_mode = "auto"` in every `pytest.ini` / `pyproject.toml`.
All test functions are `async def` automatically — **do not add
`@pytest.mark.asyncio`**:

```python
# ✅
async def test_publishes_event() -> None:
    bus = InMemoryEventBus()
    ...

# ❌ — redundant decorator
@pytest.mark.asyncio
async def test_publishes_event() -> None: ...
```

### Only mock external services
Unit tests must run without any network, file system, or process dependencies.
Mock only at the **system boundary** — never mock your own code:

| Boundary | What to mock | What NOT to mock |
|---|---|---|
| Event broker | `InMemoryEventBus` replaces Kafka/Redis bus | `AbstractEventBus` methods |
| Database | In-memory repository | `AsyncRepository.save()` directly |
| DLQ | `InMemoryDeadLetterQueue` | Your own DLQ routing logic |
| External HTTP | `unittest.mock.AsyncMock` on the HTTP client | Internal service methods |

Prefer the built-in in-memory fakes (`InMemoryEventBus`, `InMemoryDeadLetterQueue`,
`InMemoryCache`) over mocking framework patches where possible.

### Tests must be fast
Unit tests must complete in milliseconds. Any test that takes > 500 ms is
either doing I/O (wrong: should be an integration test) or has a timing
dependency (wrong: make it deterministic):

```python
# ❌ Sleeping in a unit test
async def test_ttl_evicts() -> None:
    cache = InMemoryCache(ttl=0.1)
    await cache.set("k", "v")
    await asyncio.sleep(0.2)  # ← real wall-clock delay
    assert await cache.get("k") is None

# ✅ Control time explicitly
async def test_ttl_evicts(freezegun_or_clock_mock) -> None:
    ...
```

### Test structure: one class per unit under test
Group tests in a class named `Test<UnitName>`. Each method tests one behaviour.
Name test methods descriptively — the name is the test's documentation:

```python
class TestCircuitBreaker:
    async def test_opens_after_threshold_failures(self) -> None: ...
    async def test_rejects_calls_when_open(self) -> None: ...
    async def test_half_open_probe_success_closes_circuit(self) -> None: ...
    async def test_half_open_probe_failure_reopens_circuit(self) -> None: ...
```

### File-level module docstring
Every test file must open with a module docstring that documents:
- What is under test
- Sections / categories of tests
- Key test strategy choices (e.g., "jitter=False to avoid real sleeping")

```python
"""
Unit tests for varco_core.resilience.circuit_breaker
=====================================================
Covers state machine transitions without any real sleeping.

Sections
--------
- CircuitBreakerConfig  — construction validation
- CircuitBreaker        — state transitions (CLOSED → OPEN → HALF_OPEN)
- @circuit_breaker      — decorator form

Test strategy
-------------
- ``recovery_timeout=0.0`` — instant HALF_OPEN transition, no real wait.
- Pure Python lambdas as callables — no mocking library required.
"""
```

### Fixtures
Declare fixtures in a `conftest.py` at the package's `tests/` level.
Scope fixtures carefully:

| Scope | When to use |
|---|---|
| `function` (default) | Stateful objects that must be reset per test |
| `module` | Expensive setup shared across a test file (e.g., a test container) |
| `session` | Read-only global config with zero state |

---

## 13. Testing — Integration Tests

### Use `testcontainers` for real services
Integration tests that require a real broker, database, or cache must use
`testcontainers` to spin up Docker containers. This removes the "works on my
machine" problem and makes CI reproducible:

```python
from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.memcached import MemcachedContainer
```

### Mark and skip by default
Every integration test must be marked `@pytest.mark.integration`. Integration
tests are **skipped by default** and run explicitly with `-m integration`.
This keeps `uv run pytest` fast for contributors without Docker:

```python
import pytest

@pytest.mark.integration
async def test_round_trip_with_real_redis() -> None:
    ...
```

### Container fixture pattern
Start the container at `module` scope to amortise startup time across all
tests in a file. Derive per-test settings from the running container:

```python
@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as container:
        yield container

@pytest.fixture
def redis_settings(redis_container: RedisContainer) -> RedisSettings:
    return RedisSettings(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
    )
```

### What integration tests must cover
An integration test must verify the end-to-end contract against a real
service — not just that your code calls the right library method:

- **Event bus**: publish an event, consume it, assert the payload is correct.
- **Cache**: write, read back, verify TTL expiry with a short `asyncio.sleep`.
- **Repository**: save an entity, retrieve it, verify field values persist.
- **DLQ**: trigger handler failures, assert events reach the DLQ topic/channel.

---

## 14. Testing — Broken Tests Policy

### Never push code with failing tests
Before opening a PR, all tests in every affected package must pass:

```bash
uv run pytest varco_core/tests/
uv run pytest varco_[affected_package]/tests/
```

A CI run with red tests blocks the merge — no exceptions.

### If you break a test you didn't write
Fix it immediately in the same PR. A test that was green before your change
and red after is your responsibility regardless of authorship.

### If you discover a pre-existing broken test
1. Check whether the failure is a **known flake** documented in `CLAUDE.md`.
   (Two pre-existing flakes are listed there and are excluded from this policy.)
2. If not documented: investigate, open a bug, and contact the original author
   immediately. Do **not** push code that leaves additional broken tests behind.
3. If you cannot fix it before your PR lands: add a `pytest.mark.xfail(strict=True)`
   with a GitHub issue reference and a clear reason. Never silently ignore.

### No `# noqa` without a comment
Suppressing a lint rule with `# noqa` is allowed only with an accompanying
comment explaining why:

```python
result = eval(expr)  # noqa: S307 — expr is validated against an allowlist above
```

---

## 15. Design Decisions

### Document non-obvious decisions inline
Every non-obvious design decision must include a `DESIGN:` block at the
point of the decision (module docstring, class docstring, or inline comment).
Include concrete tradeoffs — not just benefits:

```
DESIGN: outbox pattern for event publishing
    ✅ Events are saved atomically in the same DB transaction as the domain entity
    ✅ Delivery is guaranteed even if the broker restarts
    ❌ Adds latency — events are delivered asynchronously by OutboxRelay
    ❌ Requires polling infrastructure (OutboxRelay background task)
    Alternative considered: publishing directly after commit — rejected because
    a broker failure would silently drop the event with no rollback path.
```

### Frozen dataclasses for configuration
All configuration objects are `@dataclass(frozen=True)`. This makes configs
safe to share across threads, hashable for use as dict keys, and impossible
to accidentally mutate after construction.

### Thread safety and async safety annotations
Every class and module that has concurrency concerns must document them
explicitly at the top of its docstring:

```
Thread safety:  ⚠️  Not thread-safe — share only within a single event loop.
Async safety:   ✅  asyncio.Lock guards all state transitions.
```

---

## 16. What Not to Do

| Anti-pattern | Why | What to do instead |
|---|---|---|
| `except Exception: pass` | Silently swallows bugs | Catch specific types; log or re-raise |
| `raise RuntimeError(...)` for domain errors | Generic, loses structured context | Define a specific exception class |
| Mutable default arguments | Shared across all callers, leads to subtle bugs | Use `None` sentinel; create inside function |
| `asyncio.Lock()` at module or `__init__` level | Raises if no event loop is running | Create lazily inside a method |
| Per-call `CircuitBreaker` or `Bulkhead` | Counter/semaphore is reset each call | Declare as a shared instance attribute |
| Wildcard imports (`from x import *`) | Pollutes namespace, defeats static analysis | Import names explicitly |
| Mocking your own code in unit tests | Makes tests test the mock, not the code | Mock only at system boundaries |
| Sleeping in unit tests (`asyncio.sleep` with real time) | Slow, non-deterministic | Control time, or move to integration tests |
| Integration tests without `@pytest.mark.integration` | Slows down the default test run | Always mark integration tests |
| `from __future__ import annotations` missing | Forward reference annotations fail | Add it as the first import in every file |
| Publishing events directly after DB commit | Broker failure silently drops the event | Use the outbox pattern |
| `AbstractEventBus` injected into services | Violates layer rule; couples service to infra | Inject `AbstractEventProducer` |
| Boolean flag parameters | Callers can't tell meaning at the call site | Split into two functions or use an enum |
| Long functions doing multiple things | Hard to test, hard to name | Extract helpers; one function, one responsibility |
| Unused `_var` names for backwards compatibility | Clutters namespace | Delete the symbol completely |
| No `DESIGN:` block for non-obvious decisions | Future contributors can't understand why | Document tradeoffs inline |
