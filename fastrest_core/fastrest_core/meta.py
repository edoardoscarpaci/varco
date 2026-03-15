"""
orm_abstraction.meta
=====================
All backend-agnostic metadata types that the user attaches to a DomainModel:

    FieldHint         — column-level hints (unique, index, max_length, nullable)
    ForeignKey        — FK reference, string or domain-class form
    PrimaryKey        — single PK strategy OR composite PK marker
    UniqueConstraint  — table-level multi-column unique
    CheckConstraint   — table-level SQL check expression
    ParsedMeta        — normalised view consumed by both factories
    MetaReader        — single reflection entry-point for both factories
    pk_field()        — boilerplate-free dataclass field for pk

Full usage example::

    from typing import Annotated
    from dataclasses import dataclass
    from uuid import UUID
    from orm_abstraction.model import DomainModel
    from orm_abstraction.meta import (
        FieldHint, ForeignKey, PrimaryKey, PKStrategy, pk_field,
        UniqueConstraint, CheckConstraint,
    )

    # ── Single UUID pk ────────────────────────────────────────────────────────
    @dataclass
    class User(DomainModel):
        pk:    Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        name:  Annotated[str,  FieldHint(max_length=120)]
        email: Annotated[str,  FieldHint(unique=True, nullable=False)]

        class Meta:
            table = "users"

    # ── Composite PK + FK + table constraints ─────────────────────────────────
    @dataclass
    class UserRole(DomainModel):
        # PrimaryKey() with no strategy = composite marker
        user_id: Annotated[int, PrimaryKey(), ForeignKey(User, field="pk")]
        role_id: Annotated[int, PrimaryKey(), ForeignKey("roles.id")]
        granted_by: Annotated[int, ForeignKey("users.id", on_delete="SET NULL")]

        class Meta:
            table       = "user_roles"
            constraints = [
                UniqueConstraint("user_id", "role_id", name="uq_user_role"),
                CheckConstraint("role_id > 0", name="ck_role_positive"),
            ]
"""

from __future__ import annotations

import dataclasses
import typing
from enum import Enum
from typing import Annotated, Any


@dataclasses.dataclass(frozen=True)
class FieldHint:
    """
    ORM-level metadata attached directly to a field's type annotation.

    Place inside ``Annotated[<type>, FieldHint(...)]`` on any domain field
    to control how the backend generates the corresponding column or document
    field.  If no ``FieldHint`` is present the backend applies sensible
    defaults derived from the type annotation alone.

    Attributes:
        unique:     Add a UNIQUE constraint (SQL) or unique index (Mongo).
        nullable:   Allow NULL / missing values.  When omitted, nullability
                    is inferred from the annotation: ``str | None`` → nullable,
                    ``str`` → not nullable.
        index:      Add a secondary index without uniqueness.
        max_length: Maximum string length → ``VARCHAR(max_length)`` in SQL.
                    Omit to use ``TEXT`` (unbounded).

    Edge cases:
        - ``unique=True`` implies an index — setting both ``unique`` and
          ``index`` raises ``ValueError`` at class definition time.
        - Multiple ``FieldHint`` instances in the same ``Annotated`` are not
          supported; only the first one found is used.

    Example::

        @dataclass
        class Product(DomainModel):
            name:  Annotated[str,   FieldHint(max_length=200)]
            slug:  Annotated[str,   FieldHint(unique=True, max_length=200)]
            price: Annotated[float, FieldHint(index=True)]
            bio:   str | None   # no hint needed for plain optional fields
    """

    unique: bool = False
    nullable: bool = True
    index: bool = False
    max_length: int | None = None

    def __post_init__(self) -> None:
        # unique already implies an index — having both is always a mistake
        if self.unique and self.index:
            raise ValueError(
                "FieldHint: 'unique=True' already implies an index. "
                "Do not set both 'unique' and 'index' on the same field."
            )
        if self.max_length is not None and self.max_length <= 0:
            raise ValueError(
                f"FieldHint: max_length must be a positive integer, "
                f"got {self.max_length!r}."
            )


# ════════════════════════════════════════════════════════════════════════════════
# PKStrategy
# ════════════════════════════════════════════════════════════════════════════════


class PKStrategy(str, Enum):
    """
    Controls how a *single* primary key value is generated.

    Not used for composite PKs — composite columns are always app-assigned.

    Members:
        INT_AUTO:     DB auto-increment integer.  Application never sets pk.
        UUID_AUTO:    ``uuid4()`` generated Python-side before INSERT.
                      pk is available immediately after save() — no extra SELECT.
        STR_ASSIGNED: Application assigns a string pk before save().
        CUSTOM:       Application assigns any value; type is free-form.

    Edge cases:
        - ``INT_AUTO`` with a non-``int`` pk type → ``TypeError`` at startup.
        - ``UUID_AUTO`` with a non-``UUID`` pk type → ``TypeError`` at startup.
        - ``STR_ASSIGNED`` / ``CUSTOM`` with ``pk=None`` at save() time →
          the repository raises ``ValueError`` before hitting the DB.
    """

    INT_AUTO = "int_auto"
    UUID_AUTO = "uuid_auto"
    STR_ASSIGNED = "str_assigned"
    CUSTOM = "custom"


# ════════════════════════════════════════════════════════════════════════════════
# PrimaryKey
# ════════════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class PrimaryKey:
    """
    Marks a field as (part of) the primary key.

    Two modes
    ---------
    **Single PK** — attach to the ``pk`` bookkeeping field with a strategy::

        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()

    **Composite PK** — attach to each business field that forms the composite
    key.  ``strategy`` must be omitted (or ``None``)::

        user_id: Annotated[int, PrimaryKey()]
        role_id: Annotated[int, PrimaryKey()]

    When composite, the domain object's ``pk`` attribute is a tuple of values
    in declaration order: ``entity.pk == (user_id_val, role_id_val)``.

    Args:
        strategy: Generation strategy for *single* PKs.  Leave ``None`` when
                  marking composite PK columns — the factory enforces this.

    Edge cases:
        - ``PrimaryKey()`` (no strategy) on the ``pk`` bookkeeping field →
          ``TypeError`` at startup; a strategy is required for single PKs.
        - ``PrimaryKey(PKStrategy.INT_AUTO)`` on a business field (not ``pk``)
          → ``TypeError``; strategies are only valid for the ``pk`` field.
        - Multiple ``PrimaryKey`` on the same field → only the first is used.
    """

    strategy: PKStrategy | None = None


# ════════════════════════════════════════════════════════════════════════════════
# ForeignKey
# ════════════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class ForeignKey:
    """
    Declares a foreign-key relationship on an ``Annotated`` field.

    Two reference forms
    -------------------
    **String form** — explicit ``"table.column"`` reference::

        author_id: Annotated[int, ForeignKey("users.id")]

    **Domain-class form** — resolved at build time via ``MetaReader``::

        author_id: Annotated[UUID, ForeignKey(User, field="pk")]

        # Resolved to: ForeignKey("users.id") using User.Meta.table + "id"
        # (the generated SA model always names the PK column "id")

    Args:
        reference:  Either a ``"table.column"`` string or a ``DomainModel``
                    subclass.  When a class is given, ``field`` selects which
                    field of that class is the target (defaults to ``"pk"``).
        field:      Only used when ``reference`` is a domain class.  Selects
                    which field on the referenced class to point at.
                    ``"pk"`` maps to the auto-generated ``"id"`` column.
        on_delete:  SQL referential action: ``"CASCADE"``, ``"SET NULL"``,
                    ``"RESTRICT"``, ``"SET DEFAULT"``, ``"NO ACTION"``.
                    ``None`` → database default (usually ``"NO ACTION"``).
        on_update:  Same options as ``on_delete`` for UPDATE events.

    Edge cases:
        - Domain-class form with ``field="pk"`` always resolves to the
          generated ``"id"`` column regardless of the PK strategy — the
          factory always names the PK column ``"id"``.
        - If the referenced domain class has not yet been registered with the
          factory, resolution raises ``KeyError`` at startup.
        - Beanie / MongoDB has no native FK enforcement — the FK hint is stored
          in ``ParsedMeta`` for documentation purposes but no index is created
          for FK fields automatically.  Add ``FieldHint(index=True)`` alongside
          ``ForeignKey`` if you need an index.
    """

    reference: str | type  # "table.column"  or  DomainModel subclass
    field: str = "pk"  # target field on the domain class
    on_delete: str | None = None
    on_update: str | None = None

    def resolve(self) -> str:
        """
        Return the ``"table.column"`` string this FK points at.

        For string references the value is returned as-is.
        For domain-class references the table name is read from ``Meta``
        and the column name is always ``"id"`` (the auto-generated PK column).

        Returns:
            ``"table.column"`` string suitable for ``sqlalchemy.ForeignKey``.

        Raises:
            TypeError: ``reference`` is neither a string nor a class.

        Edge cases:
            - ``field != "pk"`` for a domain-class reference → resolves to
              ``"table.<field>"`` treating the field as a column name.
              Only makes sense for fields that exist as columns in the
              generated ORM class.
        """
        if isinstance(self.reference, str):
            return self.reference

        if isinstance(self.reference, type):
            meta = getattr(self.reference, "Meta", None)
            table = getattr(meta, "table", self.reference.__name__.lower())
            # "pk" always maps to the auto-generated "id" column
            column = "id" if self.field == "pk" else self.field
            return f"{table}.{column}"

        raise TypeError(
            f"ForeignKey.reference must be a string or a DomainModel subclass, "
            f"got {type(self.reference)!r}."
        )


# ════════════════════════════════════════════════════════════════════════════════
# Table-level constraints
# ════════════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class UniqueConstraint:
    """
    Backend-agnostic multi-column unique constraint.

    Maps to:
    - SA: ``sqlalchemy.UniqueConstraint(*fields, name=name)`` in ``__table_args__``
    - Beanie: ``pymongo.IndexModel([...], unique=True, name=name)`` in
      ``Settings.indexes``

    Args:
        fields: Column / field names that form the unique key.  At least two
                fields are required — use ``FieldHint(unique=True)`` for
                single-column uniques.
        name:   Optional constraint name.  Recommended for production schemas
                so that ``ALTER TABLE DROP CONSTRAINT <name>`` works reliably.

    Edge cases:
        - Fewer than 2 fields → ``ValueError`` at class definition time.
        - A field name that does not exist on the domain class → ``ValueError``
          at ``MetaReader.read()`` time (startup), not at query time.
    """

    fields: tuple[str, ...]
    name: str | None = None

    def __init__(self, *fields: str, name: str | None = None) -> None:
        # frozen=True means we must use object.__setattr__ for init
        object.__setattr__(self, "fields", tuple(fields))
        object.__setattr__(self, "name", name)
        if len(self.fields) < 2:
            raise ValueError(
                f"UniqueConstraint requires at least 2 fields, "
                f"got {list(self.fields)}. "
                "Use FieldHint(unique=True) for single-column uniqueness."
            )

    def __hash__(self) -> int:
        return hash((self.fields, self.name))


@dataclasses.dataclass(frozen=True)
class CheckConstraint:
    """
    Backend-agnostic SQL CHECK constraint.

    Maps to:
    - SA: ``sqlalchemy.CheckConstraint(condition, name=name)`` in ``__table_args__``
    - Beanie / MongoDB: **silently ignored** — MongoDB has no CHECK constraints.
      A warning is emitted at startup if ``BeanieModelFactory`` encounters one.

    Args:
        condition: SQL boolean expression.  Column names must match domain field
                   names (which equal the generated column names).
        name:      Optional constraint name.

    Edge cases:
        - ``condition`` is not validated at startup — an invalid expression
          causes a DB error on the first DDL execution (``create_all``).
        - On MongoDB backends this constraint is a no-op; document your schema
          invariants via Pydantic validators on the Beanie Document instead.
    """

    condition: str
    name: str | None = None


# ════════════════════════════════════════════════════════════════════════════════
# pk_field helper
# ════════════════════════════════════════════════════════════════════════════════


def pk_field(*, init: bool = False) -> Any:
    """
    Pre-configured dataclass field descriptor for the ``pk`` attribute.

    Hides the ``field(default=None, init=..., repr=True, compare=True)``
    boilerplate and exposes ``init`` for strategies where the application
    assigns the pk at construction time.

    Args:
        init: Whether ``pk`` should appear as a constructor argument.

              ``False`` (default) — use for ``INT_AUTO`` and ``UUID_AUTO``
              where the pk is assigned by the DB or mapper after INSERT::

                  pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
                  user = User(name="Edo", email="...")   # pk not in __init__

              ``True`` — use for ``STR_ASSIGNED`` and ``CUSTOM`` where the
              application provides the value at creation time::

                  pk: Annotated[str, PrimaryKey(PKStrategy.STR_ASSIGNED)] = pk_field(init=True)
                  tag = Tag(pk="python", name="Python")  # clean, no __setattr__

    Returns:
        ``dataclasses.field(default=None, init=init, repr=True, compare=True)``

    Edge cases:
        - ``init=True`` keeps ``default=None`` so ``pk`` is optional in the
          constructor (can be omitted).  The repository raises ``ValueError``
          at ``save()`` time if ``pk`` is still ``None`` for a
          ``STR_ASSIGNED`` entity — fail-fast at the operation boundary.
        - Works on ``frozen=True`` dataclasses for the ``init=False`` case
          because the mapper uses ``object.__setattr__`` post-construction.
    """
    return dataclasses.field(default=None, init=init, repr=True, compare=True)


# ════════════════════════════════════════════════════════════════════════════════
# ParsedMeta
# ════════════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class ParsedMeta:
    """
    Normalised view of a ``DomainModel``'s metadata — consumed by both factories.

    Produced by ``MetaReader.read()``.  Backends read from this object only;
    they never call ``getattr`` on the domain class directly.

    Attributes:
        table:               Table / collection name.
        pk_type:             Bare Python type of the single PK, or ``None``
                             for composite PKs.
        pk_strategy:         Generation strategy for single PKs, or ``None``
                             for composite PKs.
        composite_pk_fields: Ordered field names that form the composite PK.
                             Empty list for single PKs.
        fields:              ``{name: FieldHint | None}`` for every business field.
        foreign_keys:        ``{name: ForeignKey}`` for fields with an FK hint.
        constraints:         Table-level ``UniqueConstraint`` / ``CheckConstraint``
                             objects from ``Meta.constraints``.
        customize:           Optional callable ``(orm_cls: type) -> None`` that
                             is called with the generated ORM class immediately
                             after it is built.  Use to add SA relationships,
                             event listeners, or Beanie validators without
                             abandoning the abstraction.
    """

    table: str
    # Single PK (mutually exclusive with composite_pk_fields)
    pk_type: type | None
    pk_strategy: PKStrategy | None
    # Composite PK (mutually exclusive with pk_type / pk_strategy)
    composite_pk_fields: list[str]
    # Per-field metadata
    fields: dict[str, FieldHint | None]
    foreign_keys: dict[str, ForeignKey]
    # Table-level metadata
    constraints: list[UniqueConstraint | CheckConstraint]
    # Post-build customisation hook
    customize: Any  # Callable[[type], None] | None

    @property
    def is_composite_pk(self) -> bool:
        """``True`` when this entity uses a composite primary key."""
        return len(self.composite_pk_fields) > 0


# ════════════════════════════════════════════════════════════════════════════════
# MetaReader
# ════════════════════════════════════════════════════════════════════════════════


class MetaReader:
    """
    Single reflection entry-point for both backend factories.

    Reads ``Meta``, ``Annotated`` field metadata, and ``Meta.constraints``
    from a ``DomainModel`` subclass and returns a normalised ``ParsedMeta``.

    All methods are static — ``MetaReader`` is a pure namespace.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ Stateless.
    """

    @staticmethod
    def read(domain_cls: type) -> ParsedMeta:
        """
        Extract all metadata from ``domain_cls`` and return ``ParsedMeta``.

        Args:
            domain_cls: Any ``DomainModel`` subclass.

        Returns:
            Fully populated ``ParsedMeta``.

        Raises:
            TypeError:  PK type incompatible with strategy, unsupported type,
                        or malformed annotation.
            ValueError: Constraint references an unknown field name, or
                        composite PK field carries a strategy.

        Edge cases:
            - No ``pk`` override and no composite PK fields → defaults to
              ``int + INT_AUTO`` (backward compatible).
            - ``Meta`` absent → table name = lowercased class name.
            - ``Meta.constraints`` absent → empty list.
            - ``Meta.customize`` absent → ``None`` (no post-build hook).
        """
        meta_cls = getattr(domain_cls, "Meta", None)
        table: str = getattr(meta_cls, "table", domain_cls.__name__.lower())

        # include_extras=True preserves Annotated wrappers — required to read
        # PrimaryKey, FieldHint, and ForeignKey from annotation metadata.
        type_hints = typing.get_type_hints(domain_cls, include_extras=True)

        # ── 1. Detect composite PK fields ─────────────────────────────────────
        # Composite PK is declared by putting PrimaryKey() (no strategy) on
        # one or more business fields — NOT on the pk bookkeeping field.
        composite_pk_fields: list[str] = []
        for f in MetaReader.domain_fields(domain_cls):
            ann = type_hints.get(f.name)
            pk_marker = MetaReader._extract_metadata(ann, PrimaryKey)
            if pk_marker is not None:
                if pk_marker.strategy is not None:
                    raise ValueError(
                        f"{domain_cls.__name__}.{f.name}: PrimaryKey on a "
                        "business field must not carry a strategy (strategy is "
                        "only valid on the 'pk' bookkeeping field). "
                        f"Got strategy={pk_marker.strategy!r}."
                    )
                composite_pk_fields.append(f.name)

        # ── 2. Single PK config (only when no composite PK) ───────────────────
        pk_type: type | None = None
        pk_strategy: PKStrategy | None = None

        if not composite_pk_fields:
            pk_ann = type_hints.get("pk")
            pk_type, pk_strategy = MetaReader._extract_single_pk(domain_cls, pk_ann)

        # ── 3. Business field hints + FK ──────────────────────────────────────
        field_hints: dict[str, FieldHint | None] = {}
        foreign_keys: dict[str, ForeignKey] = {}

        for f in MetaReader.domain_fields(domain_cls):
            ann = type_hints.get(f.name)
            field_hints[f.name] = MetaReader._extract_metadata(ann, FieldHint)
            fk = MetaReader._extract_metadata(ann, ForeignKey)
            if fk is not None:
                foreign_keys[f.name] = fk

        # ── 4. Table-level constraints ─────────────────────────────────────────
        raw_constraints = getattr(meta_cls, "constraints", [])
        constraints = MetaReader._validate_constraints(
            domain_cls,
            raw_constraints,
            valid_names={f.name for f in MetaReader.domain_fields(domain_cls)},
        )

        # ── 5. Post-build customisation hook ──────────────────────────────────
        # Meta.customize must be a staticmethod or plain callable — it receives
        # the generated ORM class so the user can add relationships, events, etc.
        customize = getattr(meta_cls, "customize", None)

        return ParsedMeta(
            table=table,
            pk_type=pk_type,
            pk_strategy=pk_strategy,
            composite_pk_fields=composite_pk_fields,
            fields=field_hints,
            foreign_keys=foreign_keys,
            constraints=constraints,
            customize=customize,
        )

    # ── Single PK extraction ──────────────────────────────────────────────────

    @staticmethod
    def _extract_single_pk(domain_cls: type, pk_ann: Any) -> tuple[type, PKStrategy]:
        """
        Extract PK type and strategy from the ``pk`` bookkeeping field.

        Defaults to ``(int, INT_AUTO)`` when no annotation is present.

        Args:
            domain_cls: Domain class — used in error messages.
            pk_ann:     Raw annotation of the ``pk`` field, or ``None``.

        Returns:
            ``(pk_type, pk_strategy)``

        Raises:
            TypeError: Declared PK type incompatible with strategy.
            ValueError: ``PrimaryKey()`` on ``pk`` without a strategy.
        """
        from uuid import UUID

        if pk_ann is None or pk_ann is Any:
            return int, PKStrategy.INT_AUTO

        if typing.get_origin(pk_ann) is not Annotated:
            # Plain type, no PrimaryKey metadata → treat as CUSTOM
            return pk_ann, PKStrategy.CUSTOM

        args = typing.get_args(pk_ann)
        raw_pk_type = args[0]
        pk_hint: PrimaryKey | None = next(
            (a for a in args[1:] if isinstance(a, PrimaryKey)), None
        )

        pk_type, _ = MetaReader.extract_inner_type(raw_pk_type)
        strategy = pk_hint.strategy if pk_hint else PKStrategy.CUSTOM

        if pk_hint is not None and pk_hint.strategy is None:
            raise ValueError(
                f"{domain_cls.__name__}.pk: PrimaryKey() on the 'pk' field "
                "must specify a strategy. "
                "Use PrimaryKey(PKStrategy.INT_AUTO) or similar."
            )

        # Type / strategy compatibility checks
        if strategy is PKStrategy.INT_AUTO and pk_type is not int:
            raise TypeError(
                f"{domain_cls.__name__}.pk: INT_AUTO requires 'int', "
                f"got {pk_type!r}."
            )
        if strategy is PKStrategy.UUID_AUTO and pk_type is not UUID:
            raise TypeError(
                f"{domain_cls.__name__}.pk: UUID_AUTO requires 'UUID', "
                f"got {pk_type!r}."
            )
        if strategy is PKStrategy.STR_ASSIGNED and pk_type is not str:
            raise TypeError(
                f"{domain_cls.__name__}.pk: STR_ASSIGNED requires 'str', "
                f"got {pk_type!r}."
            )

        return pk_type, strategy

    # ── Constraint validation ─────────────────────────────────────────────────

    @staticmethod
    def _validate_constraints(
        domain_cls: type,
        raw: list,
        valid_names: set[str],
    ) -> list[UniqueConstraint | CheckConstraint]:
        """
        Validate ``Meta.constraints`` entries.

        Args:
            domain_cls:  Used in error messages.
            raw:         Raw list from ``Meta.constraints``.
            valid_names: Set of valid field names for ``UniqueConstraint``
                         field reference validation.

        Returns:
            Validated list of constraint objects.

        Raises:
            TypeError:  An entry is not a ``UniqueConstraint`` or
                        ``CheckConstraint``.
            ValueError: A ``UniqueConstraint`` references an unknown field.
        """
        validated: list[UniqueConstraint | CheckConstraint] = []
        for c in raw:
            if not isinstance(c, (UniqueConstraint, CheckConstraint)):
                raise TypeError(
                    f"{domain_cls.__name__}.Meta.constraints contains "
                    f"{type(c).__name__!r} which is not a UniqueConstraint "
                    "or CheckConstraint."
                )
            if isinstance(c, UniqueConstraint):
                unknown = set(c.fields) - valid_names
                if unknown:
                    raise ValueError(
                        f"{domain_cls.__name__}.Meta.constraints: "
                        f"UniqueConstraint references unknown fields: "
                        f"{sorted(unknown)}. Valid: {sorted(valid_names)}."
                    )
            validated.append(c)
        return validated

    # ── Shared annotation helpers ─────────────────────────────────────────────

    @staticmethod
    def extract_inner_type(annotation: Any) -> tuple[Any, bool]:
        """
        Strip ``Annotated`` and ``Optional`` / ``| None`` wrappers.

        Returns the bare inner type and an optionality flag.

        Handles:
            - ``str``                                → ``(str, False)``
            - ``str | None``                         → ``(str, True)``
            - ``Annotated[str, FieldHint(...)]``     → ``(str, False)``
            - ``Annotated[str | None, FieldHint(...)]`` → ``(str, True)``
        """
        # Strip Annotated first — metadata lives in args[1:]
        if typing.get_origin(annotation) is Annotated:
            annotation = typing.get_args(annotation)[0]

        # Strip Optional / | None
        import types as _types

        if isinstance(annotation, _types.UnionType):
            args = annotation.__args__
        elif typing.get_origin(annotation) is typing.Union:
            args = typing.get_args(annotation)
        else:
            return annotation, False

        non_none = [a for a in args if a is not type(None)]
        is_optional = len(non_none) < len(args)
        inner = non_none[0] if len(non_none) == 1 else typing.Union[tuple(non_none)]
        return inner, is_optional

    @staticmethod
    def _extract_metadata(annotation: Any, meta_type: type) -> Any | None:
        """
        Pull the first instance of ``meta_type`` from ``Annotated`` metadata.

        Returns ``None`` if the annotation is not ``Annotated`` or contains
        no instance of ``meta_type``.

        Args:
            annotation: Raw type annotation.
            meta_type:  The class to search for in the metadata args.
        """
        if typing.get_origin(annotation) is not Annotated:
            return None
        for meta in typing.get_args(annotation)[1:]:
            if isinstance(meta, meta_type):
                return meta
        return None

    @staticmethod
    def domain_fields(domain_cls: type) -> list[dataclasses.Field]:
        """
        Return user-declared business fields, excluding ``pk`` and ``_*``
        bookkeeping fields.
        """
        return [
            f
            for f in dataclasses.fields(domain_cls)
            if f.name != "pk" and not f.name.startswith("_")
        ]
