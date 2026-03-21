"""
fastrest_core.dto_factory
==========================
Auto-generates ``CreateDTO``, ``ReadDTO``, and ``UpdateDTO`` from a
``DomainModel`` subclass using the field metadata already declared on it.

This eliminates the boilerplate of writing three near-identical Pydantic
classes by hand.  The factory introspects the dataclass fields and their
``Annotated`` metadata to build type-correct Pydantic models at import time.

Usage
-----
**Explicit (inline assignment):**

    from fastrest_core.dto_factory import generate_dtos

    DTOs = generate_dtos(Article)
    ArticleCreateDTO = DTOs.create
    ArticleReadDTO   = DTOs.read
    ArticleUpdateDTO = DTOs.update

**Declarative (``Meta.auto_dto = True``):**

    @register
    @dataclass
    class Article(AuditedDomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str
        body: str
        published: bool = False

        class Meta:
            table    = "articles"
            auto_dto = True   # ← triggers automatic generation

    # Accessible via the class attribute stamped by @register:
    Article.DTOs.create   # → ArticleCreateDTO
    Article.DTOs.read     # → ArticleReadDTO
    Article.DTOs.update   # → ArticleUpdateDTO

Generation rules
----------------
``CreateDTO``
    - Includes every field where ``init=True`` and the field name is **not**
      ``pk`` (the bookkeeping PK field is always excluded regardless of
      ``init`` — ``STR_ASSIGNED`` makes ``pk`` an init param, but users should
      not accept a raw PK from an HTTP payload).
    - Private fields (``_*``) are always excluded.
    - ``init=False`` fields (``created_at``, ``updated_at``,
      ``definition_version``, ``row_version``) are excluded because they are
      managed by the mapper/framework, never by the caller.
    - Fields already typed as ``T | None`` default to ``None``.
    - Fields with a concrete dataclass default carry that default over.
    - Fields with a ``default_factory`` are wrapped in
      ``pydantic.Field(default_factory=...)``.
    - All other fields are required (``...``).

``UpdateDTO``
    - Same field set as ``CreateDTO``.
    - Every field becomes ``T | None = None`` so callers send only the fields
      they intend to change (sparse update pattern).
    - Inherits the ``op: UpdateOperation`` field from the ``UpdateDTO`` base.

``ReadDTO``
    - Includes every field except ``pk`` (exposed as ``id: str`` by the base)
      and internal bookkeeping (``_raw_orm``).
    - ``created_at`` / ``updated_at`` come from the ``ReadDTO`` base and are
      **not** re-declared here.
    - ``definition_version``, ``row_version``, ``tenant_id``, and any other
      non-excluded fields are included with their exact domain type.
    - Fields typed as ``T | None`` in the domain model are optional (``None``
      default); all other included fields are required.

DESIGN: ``pydantic.create_model`` instead of ``type()`` / ``exec``
  ✅ Pydantic's own factory — no metaclass or __init_subclass__ surprises.
  ✅ Validates field specs at creation time (bad types raise immediately).
  ✅ Full Pydantic feature support (validators, JSON schema, etc.) on the
     generated class.
  ❌ Slightly less readable stack traces than hand-written classes — the class
     is created in this module, not at the call site.

DESIGN: ``Annotated[T, FieldHint(...)]`` is stripped to bare ``T``
  ✅ Pydantic does not understand ``FieldHint`` / ``ForeignKey`` / ``PrimaryKey``
     — passing them through would either be a no-op or raise a validation error.
  ✅ The raw Python type ``T`` is exactly what Pydantic needs for field creation.
  ❌ Any Pydantic-compatible annotations inside ``Annotated`` (e.g.
     ``annotated_types.Gt``) would also be stripped — do not mix framework
     metadata with Pydantic metadata in the same ``Annotated``.

Thread safety:  ✅ ``generate_dtos`` is a pure function; it reads class
                    metadata but never writes shared state.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

import dataclasses
import typing
from dataclasses import MISSING
from typing import Annotated, Any, NamedTuple

from pydantic import Field, create_model

from fastrest_core.dto.base import CreateDTO, ReadDTO, UpdateDTO
from fastrest_core.meta import MetaReader

# ── Constants ─────────────────────────────────────────────────────────────────

# Fields that are always excluded from CreateDTO / UpdateDTO.
#
# We rely on ``f.init`` as the primary gate (mapper-managed fields all have
# ``init=False``), but ``pk`` is special: ``STR_ASSIGNED`` makes it
# ``init=True``, yet we still must not expose a raw PK field on an HTTP payload
# — the service layer assigns or validates it.
_ALWAYS_EXCLUDE_FROM_CREATE: frozenset[str] = frozenset({"pk"})

# Fields declared on the ReadDTO base class that must NOT be re-declared in the
# generated subclass — Pydantic raises on duplicate field names.
#
# Note: ``pk`` is intentionally NOT in this set.  The base declares ``pk: Any``
# as a broad placeholder; the generated subclass overrides it with the concrete
# domain type (e.g. ``pk: UUID``) so type checkers and Pydantic validators see
# the right type.
_READ_BASE_FIELDS: frozenset[str] = frozenset({"created_at", "updated_at"})


# ── DTOSet ────────────────────────────────────────────────────────────────────


class DTOSet(NamedTuple):
    """
    Container for the three auto-generated DTO classes produced by
    ``generate_dtos()``.

    Attributes:
        create: Pydantic model for POST / create payloads.
        read:   Pydantic model for GET / read responses.
        update: Pydantic model for PATCH / PUT payloads.

    Thread safety:  ✅ Immutable NamedTuple — safe to share across threads.
    Async safety:   ✅ No mutable state.
    """

    create: type[CreateDTO]
    read: type[ReadDTO]
    update: type[UpdateDTO]


# ── Internal helpers ──────────────────────────────────────────────────────────


def _strip_annotated(ann: Any) -> Any:
    """
    Strip the outer ``Annotated[T, ...]`` wrapper if present and return ``T``.

    Pydantic does not understand ``FieldHint``, ``ForeignKey``, or
    ``PrimaryKey`` metadata — passing the raw ``Annotated`` form would either
    silently ignore the metadata (Pydantic v2 passes unknown metadata through)
    or, in edge cases, raise.  Stripping to the bare type is the safe choice.

    Args:
        ann: A raw field annotation, possibly ``Annotated[T, metadata...]``.

    Returns:
        The unwrapped type.  Non-Annotated annotations are returned unchanged.

    Edge cases:
        - Doubly-wrapped ``Annotated`` (rare, but valid Python) — only the
          outermost layer is stripped.  The inner type may still be
          ``Annotated``; Pydantic handles that fine for its own annotations.
        - ``ann`` is ``None`` or any non-type object — returned as-is; the
          caller is responsible for providing a valid annotation.
    """
    if typing.get_origin(ann) is Annotated:
        # args[0] is the base type; args[1:] are the metadata objects
        return typing.get_args(ann)[0]
    return ann


# ── Public API ────────────────────────────────────────────────────────────────


def generate_dtos(domain_cls: type) -> DTOSet:
    """
    Auto-generate ``CreateDTO``, ``ReadDTO``, and ``UpdateDTO`` from a
    ``DomainModel`` subclass.

    The generated class names follow the pattern
    ``{DomainClassName}CreateDTO``, ``{DomainClassName}ReadDTO``,
    ``{DomainClassName}UpdateDTO``.

    Args:
        domain_cls: A ``DomainModel`` subclass decorated with ``@dataclass``.
                    Must be fully formed (i.e. ``@dataclass`` must have already
                    run — apply ``@register`` / ``@dataclass`` before calling
                    this function or use ``Meta.auto_dto = True`` which is
                    triggered by ``@register``).

    Returns:
        A ``DTOSet`` namedtuple with ``.create``, ``.read``, ``.update``
        attributes each pointing at a Pydantic model class.

    Raises:
        TypeError: ``domain_cls`` is not a dataclass.

    Edge cases:
        - ``pk`` is always excluded from CreateDTO / UpdateDTO even when
          ``init=True`` (``STR_ASSIGNED`` strategy).
        - Private fields (``_*``) are always excluded from all three DTOs.
        - ``T | None`` fields with no explicit default get ``None`` as the
          default in CreateDTO (consistent with Python typing convention).
        - ``default_factory`` fields (e.g. ``list``, ``frozenset``) are wrapped
          in ``pydantic.Field(default_factory=...)`` so Pydantic does not share
          a mutable default across instances.
        - ``FieldHint``, ``ForeignKey``, ``PrimaryKey`` metadata inside
          ``Annotated`` is stripped — Pydantic does not need it.
        - ``tenant_id`` (from ``TenantMixin``) is included in CreateDTO /
          UpdateDTO because it has ``init=True``.  If your API should not
          accept it from callers, subclass the generated DTO and exclude it.
        - ``definition_version`` and ``row_version`` (from
          ``VersionedDomainModel``) are excluded from CreateDTO / UpdateDTO
          because they have ``init=False``.  They ARE included in ReadDTO.

    Thread safety:  ✅ Pure function — reads class metadata, writes nothing.
    Async safety:   ✅ No I/O.

    Example::

        from dataclasses import dataclass
        from typing import Annotated
        from uuid import UUID
        from fastrest_core import AuditedDomainModel, register
        from fastrest_core.meta import PrimaryKey, PKStrategy, pk_field
        from fastrest_core.dto_factory import generate_dtos

        @register
        @dataclass
        class Article(AuditedDomainModel):
            pk:        Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
            title:     str
            body:      str
            published: bool = False

            class Meta:
                table = "articles"

        DTOs = generate_dtos(Article)
        # DTOs.create → ArticleCreateDTO
        #   title: str  (required)
        #   body: str   (required)
        #   published: bool = False

        # DTOs.update → ArticleUpdateDTO
        #   op: UpdateOperation = REPLACE  (from base)
        #   title: str | None = None
        #   body: str | None = None
        #   published: bool | None = None

        # DTOs.read → ArticleReadDTO
        #   id: str             (from ReadDTO base)
        #   created_at: datetime (from ReadDTO base)
        #   updated_at: datetime (from ReadDTO base)
        #   title: str
        #   body: str
        #   published: bool
    """
    if not dataclasses.is_dataclass(domain_cls):
        raise TypeError(
            f"generate_dtos() requires a @dataclass-decorated class, "
            f"got {domain_cls!r}. "
            "Ensure @dataclass is applied before calling generate_dtos() "
            "or before the @register decorator that triggers auto_dto."
        )

    name = domain_cls.__name__

    # include_extras=True keeps Annotated wrappers so _strip_annotated can
    # distinguish FieldHint-annotated fields from plain ones.
    type_hints = typing.get_type_hints(domain_cls, include_extras=True)
    all_fields = dataclasses.fields(domain_cls)

    # ── Build CreateDTO / UpdateDTO field sets ────────────────────────────────

    # "Business field" for create/update: init=True, not pk, not private.
    # This implicitly excludes created_at / updated_at / definition_version /
    # row_version because the base classes declare them all as init=False.
    create_fields: dict[str, Any] = {}
    update_fields: dict[str, Any] = {}

    for f in all_fields:
        # Exclude pk (always) and private bookkeeping fields (_raw_orm, etc.)
        if f.name in _ALWAYS_EXCLUDE_FROM_CREATE or f.name.startswith("_"):
            continue
        # Exclude mapper-managed fields — they are never supplied by callers
        if not f.init:
            continue

        ann = type_hints.get(f.name, Any)
        # Strip FieldHint / ForeignKey / PrimaryKey so Pydantic gets bare type
        clean = _strip_annotated(ann)
        inner, is_optional = MetaReader.extract_inner_type(clean)

        # ── CreateDTO: preserve original optionality and defaults ─────────────
        if f.default is not MISSING:
            # Concrete default (e.g. ``published: bool = False``)
            create_fields[f.name] = (clean, f.default)
        elif f.default_factory is not MISSING:  # type: ignore[misc]
            # Factory default (e.g. ``tags: list[str] = field(default_factory=list)``)
            # Wrap in Field() so Pydantic does not share the mutable default
            create_fields[f.name] = (clean, Field(default_factory=f.default_factory))
        elif is_optional:
            # ``T | None`` with no explicit default — conventionally None
            create_fields[f.name] = (clean, None)
        else:
            # Genuinely required field — Ellipsis signals "no default" to Pydantic
            create_fields[f.name] = (clean, ...)

        # ── UpdateDTO: everything is optional (sparse update pattern) ─────────
        # Strip any existing | None from the inner type then re-apply | None
        # so we never get ``str | None | None`` from an already-optional field.
        update_fields[f.name] = (inner | None, None)

    # ── Build ReadDTO field set ───────────────────────────────────────────────

    # The ReadDTO base declares: pk (Any), created_at, updated_at.
    #
    # Strategy:
    #   1. Override ``pk: Any`` with the exact domain PK type so the generated
    #      DTO is fully typed (e.g. ``pk: UUID`` instead of ``pk: Any``).
    #      For composite PKs, the individual PK fields are treated as regular
    #      business fields and included in the main loop below.
    #   2. Skip ``created_at`` / ``updated_at`` — they come from the base.
    #   3. Skip ``_raw_orm`` and any private ``_*`` fields (internal only).
    #   4. Include all remaining fields with their exact domain types.
    read_fields: dict[str, Any] = {}

    # Step 1: concrete PK type (single PK only — composite is handled below)
    meta = MetaReader.read(domain_cls)
    if not meta.is_composite_pk:
        # Override the base's broad ``pk: Any`` with the real type (UUID, int, str)
        read_fields["pk"] = (meta.pk_type, ...)

    for f in all_fields:
        # Always skip private/internal fields
        if f.name.startswith("_"):
            continue
        # Skip fields whose canonical form comes from the ReadDTO base class
        if f.name in _READ_BASE_FIELDS:
            continue
        # ``pk`` is already handled above for single-PK entities; skip it here
        # to avoid a duplicate field definition in the generated class.
        if f.name == "pk" and not meta.is_composite_pk:
            continue

        ann = type_hints.get(f.name, Any)
        clean = _strip_annotated(ann)
        _, is_optional = MetaReader.extract_inner_type(clean)

        # Preserve optionality from the domain model; required fields stay required
        if is_optional:
            read_fields[f.name] = (clean, None)
        else:
            read_fields[f.name] = (clean, ...)

    # ── Dynamically create the three Pydantic model classes ──────────────────

    CreateDTOCls: type[CreateDTO] = create_model(
        f"{name}CreateDTO",
        __base__=CreateDTO,
        **create_fields,
    )

    UpdateDTOCls: type[UpdateDTO] = create_model(
        f"{name}UpdateDTO",
        __base__=UpdateDTO,
        **update_fields,
    )

    ReadDTOCls: type[ReadDTO] = create_model(
        f"{name}ReadDTO",
        __base__=ReadDTO,
        **read_fields,
    )

    return DTOSet(
        create=CreateDTOCls,
        read=ReadDTOCls,
        update=UpdateDTOCls,
    )
