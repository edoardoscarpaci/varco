"""
varco_conformance
==================

Shared, test-only conformance suites for ``varco_core`` ABCs
(``AbstractEventBus``, ``CacheBackend``, ``AbstractJobStore``,
``AbstractDeadLetterQueue``, ``ChannelManager``, ``AbstractIdempotencyStore``,
``WebhookSubscriptionRepository``, ``AbstractTokenRevocationStore``).

This package lives at the repo root under ``testkit/`` — it is **never**
packaged or published, and is reached only via each participating package's
``pythonpath = ["../testkit"]`` pytest ini setting (Plan 012 / RT6, Open
Question 2).

Contract for every class in this package:

- Never named ``Test*`` — pytest's default collection only picks up
  ``Test*`` classes, so these abstract base classes are never collected
  standalone. A backend opts in by subclassing and naming its concrete
  subclass ``Test<Backend><Thing>Conformance``.
- Every abstract fixture (the thing under test, e.g. ``bus``/``cache``/
  ``store``/``dlq``) raises ``NotImplementedError`` by default — a backend
  subclass that forgets to override the fixture fails loudly and
  immediately, rather than silently skipping the whole suite.
- A red run means one of three things, and only one may edit this package: a
  genuine backend ABC violation (``@pytest.mark.xfail(strict=True)`` on the
  consuming per-backend test module, with a ``reason=`` naming its finding
  ID — never an in-place production fix, never a weakened assertion here), a
  gap in a suite itself (fix it in this package, no marker), or a legitimate
  backend capability divergence (override the one test in the subclass, with
  a docstring). See ``COVERAGE.md``'s **Conformance findings register** for
  the full decision table, an in-tree example of each, and where
  accumulated findings live — not BACKLOG.md, which is trimmed by design
  (Plan 012 Non-goals; Plan 042).
"""

from __future__ import annotations
