"""
Workspace-wide DI declaration health checks.

Why this file lives in ``varco_fastapi/tests``
----------------------------------------------
It has to import *every* varco package at once (``varco_core`` must not depend
on its siblings), and ``varco_fastapi`` is the package that already sits on top
of the whole stack.

What it guards
--------------
A user reported ``TypeError: tracer_provider() missing 1 required positional
argument: 'config'`` when ``VarcoFastAPIConfiguration`` and ``OtelConfiguration``
shared one container — ``Inject[OtelConfig]`` was silently not injected.

Root cause (in providify, not varco):

1. ``ProviderBinding.__init__`` (``providify/binding.py``) falls back to
   ``eval(fn.__annotations__["return"], fn.__globals__)`` when
   ``get_type_hints()`` fails.  Under PEP 563 a *quoted* annotation
   ``-> "Foo"`` is stored as the string ``"'Foo'"``, so that ``eval`` returns
   the **string** ``'Foo'`` — and a binding whose ``interface`` is a ``str``
   gets registered without complaint.
2. ``DIContainer._build_localns()`` (``providify/container.py``) then does
   ``binding.interface.__name__`` → ``AttributeError: 'str' object has no
   attribute '__name__'``.
3. ``DIContainer._collect_kwargs_sync()`` wraps that in
   ``except Exception: hints = {}`` — so **every** provider and constructor in
   the container is called with **zero** injected arguments from then on.

One malformed provider anywhere therefore disables injection container-wide,
and the only visible symptom is a baffling ``missing N required positional
arguments`` on some *unrelated* provider.  varco cannot repair providify from
here, but it must never ship a binding that triggers it.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import inspect
import pkgutil

import pytest
from opentelemetry.sdk.trace import TracerProvider
from providify import DIContainer, Provider

# Module-scope imports on purpose: with ``from __future__ import annotations``
# a ``@Provider``'s return annotation is a lazy string that providify resolves
# against ``fn.__globals__``.  A type imported inside a test function is absent
# from those globals and ``container.provide()`` fails at registration time.
from varco_core.observability.config import OtelConfig
from varco_core.observability.di import OtelConfiguration
from varco_fastapi.di import VarcoFastAPIConfiguration

#: `eval(annotation, fn.__globals__)` also implicitly sees `__builtins__` —
#: a bare `-> object`/`-> str`/... nested @Provider return annotation is
#: always resolvable regardless of what the enclosing function imports.
_BUILTIN_NAMES = frozenset(dir(builtins))

VARCO_PACKAGES = (
    "varco_core",
    "varco_fastapi",
    "varco_sa",
    "varco_redis",
    "varco_kafka",
    "varco_beanie",
    "varco_casbin",
    "varco_nats",
    "varco_memcached",
    "varco_ws",
)


def _iter_modules():
    """Yield every importable module of every varco package."""
    for pkg_name in VARCO_PACKAGES:
        try:
            pkg = importlib.import_module(pkg_name)
        except ImportError:  # optional backend deps absent — nothing to check
            continue
        yield pkg
        for info in pkgutil.walk_packages(pkg.__path__, prefix=f"{pkg_name}."):
            try:
                yield importlib.import_module(info.name)
            except ImportError:
                continue


def _iter_provider_callables():
    """Yield ``(qualified_name, source_location, fn)`` for every ``@Provider``."""
    from providify.metadata import _get_provider_metadata

    seen: set[int] = set()
    for mod in _iter_modules():
        for name, obj in vars(mod).items():
            if getattr(obj, "__module__", None) != mod.__name__:
                continue
            candidates = []
            if inspect.isfunction(obj):
                candidates.append((f"{mod.__name__}.{name}", obj))
            elif inspect.isclass(obj):
                candidates.extend(
                    (f"{mod.__name__}.{name}.{m}", fn)
                    for m, fn in vars(obj).items()
                    if inspect.isfunction(fn)
                )
            for qual, fn in candidates:
                if id(fn) in seen or _get_provider_metadata(fn) is None:
                    continue
                seen.add(id(fn))
                try:
                    loc = f"{inspect.getsourcefile(fn)}:{inspect.getsourcelines(fn)[1]}"
                except OSError:  # pragma: no cover - source-less callable
                    loc = "<unknown>"
                yield qual, loc, fn


class TestProviderReturnAnnotations:
    def test_regression_no_provider_uses_a_quoted_return_annotation(self) -> None:
        """
        A quoted return annotation is the one shape that makes providify
        register a ``str`` interface (see module docstring, step 1), which
        silently disables injection for the whole container.

        Every varco module already has ``from __future__ import annotations``,
        so quoting is never needed — the annotation is lazy either way.  Import
        the type at module scope and drop the quotes instead.
        """
        offenders = [
            f"{qual} at {loc} -> {fn.__annotations__.get('return')!r}"
            for qual, loc, fn in _iter_provider_callables()
            if isinstance(fn.__annotations__.get("return"), str)
            and fn.__annotations__["return"].strip().startswith(("'", '"'))
        ]
        assert not offenders, (
            "@Provider return annotations must not be quoted under PEP 563 — "
            "providify registers the resulting string as the binding interface "
            "and every Inject[...] in the container is then silently dropped:\n  "
            + "\n  ".join(offenders)
        )

    def test_regression_every_provider_return_type_resolves_to_a_type(self) -> None:
        """
        The generalisation of the test above: whatever providify ends up using
        as the binding interface must be a real type (or a generic alias), not
        a string.
        """
        from typing import get_origin, get_type_hints

        offenders = []
        for qual, loc, fn in _iter_provider_callables():
            try:
                iface = get_type_hints(fn).get("return")
            except Exception as exc:  # noqa: BLE001 - the failure IS the finding
                offenders.append(f"{qual} at {loc}: get_type_hints failed: {exc}")
                continue
            if iface is None:
                offenders.append(f"{qual} at {loc}: no resolvable return type")
            elif not isinstance(iface, type) and get_origin(iface) is None:
                offenders.append(f"{qual} at {loc}: interface {iface!r} is not a type")
        assert not offenders, "\n  ".join(["Broken @Provider return types:", *offenders])


class TestContainerLocalnsHealth:
    """
    ``DIContainer._build_localns()`` is the single choke point: if it raises,
    every ``Inject[...]`` parameter in the container is silently dropped.
    """

    def test_localns_build_never_raises_for_a_full_varco_container(self) -> None:
        # Plan 045: setup_varco_defaults() is gone; scanning "varco_fastapi"
        # (in VARCO_PACKAGES) already auto-installs VarcoFastAPIConfiguration, so
        # the redundant explicit install() below is dropped too.
        container = DIContainer()
        for pkg in VARCO_PACKAGES:
            try:
                container.scan(pkg, recursive=True)
            except ImportError:  # pragma: no cover - optional backend absent
                continue
        container.install(OtelConfiguration)

        container._build_localns()  # must not raise

        poisoned = [b for b in container._bindings if isinstance(b.interface, str)]
        assert not poisoned, f"str-interface bindings registered: {poisoned!r}"


@Provider(singleton=True)
def _composite_otel_config() -> OtelConfig:
    """App-supplied ``OtelConfig`` — module scope so its annotation resolves."""
    return OtelConfig(service_name="composite-svc")


class TestVarcoFastApiModuleWithOtelConfiguration:
    """The literal scenario from the bug report."""

    @pytest.mark.parametrize("otel_first", [False, True])
    def test_regression_tracer_provider_resolves_with_both_modules(self, otel_first: bool) -> None:
        otel_config = _composite_otel_config

        container = DIContainer()
        container.provide(otel_config)
        if otel_first:
            container.install(OtelConfiguration)
            container.install(VarcoFastAPIConfiguration)
        else:
            container.install(VarcoFastAPIConfiguration)
            container.install(OtelConfiguration)

        provider = container.get(TracerProvider)

        assert isinstance(provider, TracerProvider)
        # Proves Inject[OtelConfig] really resolved rather than being dropped.
        assert provider.resource.attributes["service.name"] == "composite-svc"


# ══════════════════════════════════════════════════════════════════════════════
# Function-local @Provider guard (sibling defect of the quoted-annotation bug)
# ══════════════════════════════════════════════════════════════════════════════
#
# DESIGN: static AST scan over the whole workspace (sources + test suites)
#     ✅ Catches the defect without importing the module — a test module that
#        needs Docker/testcontainers can still be linted here, which is exactly
#        where the bug hid: ``varco_redis/tests/test_redis_bulkhead.py``
#        declared a function-local ``@Provider`` returning a type imported
#        inside the test body, and only ever ran under ``-m integration``.
#     ✅ Covers test suites too, which ``_iter_provider_callables()`` above
#        deliberately does not (it walks *package* modules only).
#     ❌ A name assigned dynamically at module scope (e.g. via ``globals()``)
#        would be a false positive — none exist, and the guard names the file
#        and line so a future one is trivial to whitelist.


def _module_level_names(tree: ast.Module) -> set[str]:
    """
    Collect every name bound at module scope (imports, assignments, defs).

    Args:
        tree: Parsed module AST.

    Returns:
        Set of names resolvable through ``fn.__globals__`` at runtime.
    """
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname or alias.name)
        elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.If):
            # ``if TYPE_CHECKING:`` blocks bind nothing at runtime, but plain
            # ``if sys.version_info >= ...`` blocks do — walk them either way
            # and let the TYPE_CHECKING case surface as a real finding.
            for sub in node.body:
                if isinstance(sub, (ast.Import, ast.ImportFrom)):
                    for alias in sub.names:
                        names.add(alias.asname or alias.name.split(".")[0])
    return names


def _annotation_patched_providers(enclosing) -> set[str]:
    """
    Names whose ``__annotations__["return"]`` is reassigned in ``enclosing``.

    ``varco_ws.di.bind_websocket_adapter`` legitimately declares a nested
    ``@Provider`` and then repairs the lazy annotation with
    ``_ws_factory.__annotations__["return"] = WebSocketEventBus`` *before*
    calling ``container.provide()`` — providify then sees a real class, so the
    defect cannot occur.  Those are exempt.

    Args:
        enclosing: The enclosing ``FunctionDef``/``AsyncFunctionDef`` node.

    Returns:
        Set of nested-function names with a repaired return annotation.
    """
    patched: set[str] = set()
    for node in ast.walk(enclosing):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            # <name>.__annotations__["return"] = <cls>
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Attribute)
                and target.value.attr == "__annotations__"
                and isinstance(target.value.value, ast.Name)
            ):
                patched.add(target.value.value.id)
    return patched


def _is_provider_decorator(node) -> bool:
    """True when ``node`` is ``@Provider`` or ``@Provider(...)``."""
    target = node.func if isinstance(node, ast.Call) else node
    if isinstance(target, ast.Name):
        return target.id == "Provider"
    if isinstance(target, ast.Attribute):
        return target.attr == "Provider"
    return False


def _returns_overridden_providers(enclosing) -> set[str]:
    """
    Names passed as the first positional argument to a
    ``<container>.provide(<name>, returns=<Interface>)`` call inside
    ``enclosing``.

    ``container.provide(fn, returns=X)`` never consults ``fn``'s own return
    annotation at all — providify's interface resolution short-circuits to
    the explicit ``returns=`` value (``ProviderBinding.__init__`` only
    falls back to the annotation when ``interface is None``). A nested
    provider registered exclusively this way structurally cannot trigger
    the annotation-eval defect this test guards against, regardless of
    what its own (never-consulted) return annotation names.

    Args:
        enclosing: The enclosing ``FunctionDef``/``AsyncFunctionDef`` node.

    Returns:
        Set of nested-function names registered only via ``returns=``.
    """
    overridden: set[str] = set()
    for node in ast.walk(enclosing):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "provide"):
            continue
        if not any(kw.arg == "returns" for kw in node.keywords):
            continue
        if node.args and isinstance(node.args[0], ast.Name):
            overridden.add(node.args[0].id)
    return overridden


def _provider_names_passed_to_provide(enclosing) -> set[str]:
    """Every bare name passed as an argument to any ``.provide(...)`` call."""
    names: set[str] = set()
    for node in ast.walk(enclosing):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "provide"):
            continue
        for arg in node.args:
            if isinstance(arg, ast.Name):
                names.add(arg.id)
    return names


def _iter_source_files():
    """Yield every ``.py`` file in every varco package (sources + tests)."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for pkg in VARCO_PACKAGES:
        pkg_root = root / pkg
        if not pkg_root.is_dir():
            continue
        for path in pkg_root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            yield path


def test_regression_no_function_local_provider_with_unresolvable_return_type() -> None:
    """
    User reports: an integration test raised ``TypeError: Provider '_settings'
    declares an unresolvable return type annotation 'RedisEventBusSettings'``.
    Correct behaviour is that every ``@Provider`` return annotation is
    resolvable, because providify evaluates it against ``fn.__globals__`` only
    — so a ``@Provider`` nested in a function body must not name a type that
    was imported inside that same body.
    """
    offenders: list[str] = []

    for path in _iter_source_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - not our file to fix
            continue

        module_names = _module_level_names(tree)

        # Walk only *nested* function definitions: a module-scope @Provider
        # always sees module globals and cannot exhibit this defect.
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            patched = _annotation_patched_providers(node)
            returns_overridden = _returns_overridden_providers(node)
            provided_names = _provider_names_passed_to_provide(node)
            for child in ast.walk(node):
                if child is node or not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not any(_is_provider_decorator(d) for d in child.decorator_list):
                    continue
                if child.name in patched or child.name in returns_overridden:
                    continue
                # A nested provider that is never passed to any
                # `.provide(...)` call at all is decoration-only (e.g. a
                # unit test inspecting the stamped `@Requires` marker) —
                # it never reaches `ProviderBinding.__init__`, so its
                # return annotation is never eval()'d and cannot poison a
                # container. Exempt.
                if child.name not in provided_names:
                    continue
                returns = child.returns
                if returns is None:
                    continue
                # Only a bare Name annotation is checkable statically; a
                # subscripted/attribute annotation resolves through its root.
                root = returns
                while isinstance(root, ast.Subscript):
                    root = root.value
                if isinstance(root, ast.Attribute):
                    while isinstance(root, ast.Attribute):
                        root = root.value
                if not isinstance(root, ast.Name):
                    continue
                if root.id in _BUILTIN_NAMES:
                    continue
                if root.id not in module_names:
                    offenders.append(
                        f"{path}:{child.lineno} — @Provider {child.name}() returns "
                        f"{root.id!r}, which is not bound at module scope"
                    )

    assert not offenders, "\n".join(
        [
            "Function-local @Provider with an unresolvable return annotation.",
            "providify resolves the (PEP 563, lazy) annotation against",
            "fn.__globals__ only — import the type at module scope and declare",
            "the @Provider at module scope too.",
            *offenders,
        ]
    )
