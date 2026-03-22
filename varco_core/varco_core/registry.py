"""
orm_abstraction.registry
=========================
Global ``DomainModelRegistry`` and the ``@register`` decorator.

Usage::

    from orm_abstraction.registry import register

    @register
    @dataclass
    class User(DomainModel):
        ...

    # At startup — pick up every @register-stamped class in a module/package:
    provider.autodiscover("myapp.models")

    # Or register just the classes from this registry directly:
    provider.register(*DomainModelRegistry.all())
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from .model import DomainModel

if TYPE_CHECKING:
    # Imported only for type checking — avoids a circular import at runtime
    # because dto_factory imports from meta which imports from model.
    from varco_core.dto.factory import DTOSet

D = TypeVar("D", bound=DomainModel)


class DomainModelRegistry:
    """
    Process-level registry that tracks every ``DomainModel`` subclass
    decorated with ``@register``.

    This is intentionally a simple ordered set — it records *which* classes
    the user opted in, nothing more.  The backend-specific ORM class
    generation still lives in the provider factories; this registry is purely
    a declaration list.

    DESIGN: class-level dict (insertion-ordered since Python 3.7)
      ✅ Declaration order is preserved — useful for deterministic DDL output
         and readable ``repr()``
      ✅ Idempotent — decorating the same class twice (e.g. via a re-import)
         does not create duplicates
      ✅ No global provider dependency — the registry is purely a list of
         classes; binding to a provider happens at ``autodiscover()`` call time
      ❌ Global mutable state — tests that register domain classes share this
         registry; use ``DomainModelRegistry.clear()`` in teardown if needed

    Thread safety:  ⚠️ Registration happens at import time (single-threaded);
                    reads after startup are safe.
    Async safety:   ✅ Read-only after startup.
    """

    # Ordered dict used as an ordered set: class → None.
    # Preserves declaration order while giving O(1) membership tests.
    _classes: dict[type, None] = {}

    @classmethod
    def add(cls, domain_cls: type[D]) -> None:
        """
        Record ``domain_cls`` in the registry.

        Called by the ``@register`` decorator — not intended for direct use.

        Args:
            domain_cls: Any ``DomainModel`` subclass.

        Edge cases:
            - Calling ``add()`` twice for the same class is a no-op —
              the class appears only once, preserving its first insertion order.
        """
        cls._classes[domain_cls] = None

    @classmethod
    def all(cls) -> list[type[DomainModel]]:
        """
        Return all registered ``DomainModel`` subclasses in declaration order.

        Returns:
            List of classes.  Empty if no class has been decorated yet.
        """
        return list(cls._classes.keys())  # type: ignore[return-value]

    @classmethod
    def clear(cls) -> None:
        """
        Remove all registered classes.

        Intended for use in test teardown to prevent cross-test contamination.

        Example::

            def teardown():
                DomainModelRegistry.clear()
        """
        cls._classes.clear()

    @classmethod
    def __repr__(cls) -> str:
        names = [c.__name__ for c in cls._classes]
        return f"DomainModelRegistry({names})"


# ── @register decorator ───────────────────────────────────────────────────────


def register(domain_cls: type[D]) -> type[D]:
    """
    Class decorator that opts a ``DomainModel`` subclass into automatic
    provider registration.

    Apply **after** ``@dataclass`` so the class is fully formed when it enters
    the registry::

        @register          # ← outer
        @dataclass         # ← inner — runs first
        class User(DomainModel):
            ...

    The decorator is a pure no-op at runtime beyond recording the class — it
    returns the class unchanged so ``isinstance``, ``issubclass``, pickling,
    and all other reflection still work normally.

    Args:
        domain_cls: The ``DomainModel`` subclass to register.

    Returns:
        ``domain_cls`` unchanged.

    Raises:
        TypeError: ``domain_cls`` is not a subclass of ``DomainModel``.

    Edge cases:
        - Applying ``@register`` to a class that is not a ``DomainModel``
          subclass raises ``TypeError`` immediately — catches mistakes at
          import time rather than at ``provider.autodiscover()`` call time.
        - Re-applying ``@register`` to an already-registered class is safe
          and idempotent.
        - Abstract base models or mixins should NOT be decorated — they would
          be passed to the factory which would fail on missing ``Meta.table``.
          Use ``Meta: skip = True`` (or simply omit ``@register``) on them.

    Example::

        @register
        @dataclass
        class User(DomainModel):
            pk:    Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
            name:  Annotated[str,  FieldHint(max_length=120)]
            email: Annotated[str,  FieldHint(unique=True, nullable=False)]

            class Meta:
                table = "users"
    """
    if not (isinstance(domain_cls, type) and issubclass(domain_cls, DomainModel)):
        raise TypeError(
            f"@register can only be applied to DomainModel subclasses, "
            f"got {domain_cls!r}. "
            "Make sure @register is placed above @dataclass."
        )
    DomainModelRegistry.add(domain_cls)

    # ── Auto-DTO generation ───────────────────────────────────────────────────
    # If the inner Meta class declares ``auto_dto = True``, generate the three
    # DTO classes immediately and attach them as ``domain_cls.DTOs``.
    #
    # DESIGN: lazy import of ``generate_dtos`` inside the function
    #   ✅ Avoids a circular import: dto_factory → meta → model → registry
    #   ✅ The import cost is paid only for classes that opt in
    #   ❌ Slightly less visible than a top-level import — see dto_factory.py
    #      for the full ``generate_dtos`` documentation.
    meta_cls = getattr(domain_cls, "Meta", None)
    if getattr(meta_cls, "auto_dto", False):
        from varco_core.dto.factory import generate_dtos  # noqa: PLC0415

        dtos: DTOSet = generate_dtos(domain_cls)
        # Stamp the DTOSet onto the class so callers can reach it via
        # ``MyModel.DTOs.create`` / ``.read`` / ``.update``.
        domain_cls.DTOs = dtos  # type: ignore[attr-defined]

    return domain_cls
