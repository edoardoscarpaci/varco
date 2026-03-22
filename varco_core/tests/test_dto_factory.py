"""
Unit tests for varco_core.dto.factory (generate_dtos / DTOSet)
and varco_core.dto.pagination (paged_response / PagedReadDTO / PageCursor).

Test strategy
-------------
- Every test defines its own inner dataclass to avoid Pydantic name collisions
  across test runs (create_model() registers names globally in Pydantic's
  internal model registry).
- DomainModelRegistry.clear() is called in an autouse fixture so that
  @register + auto_dto = True tests don't bleed into each other.
- QueryParams is constructed directly (frozen dataclass, no I/O) to exercise
  the paged_response() helper without standing up any service layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone as _tz
from typing import Annotated
from uuid import UUID

import pytest
from pydantic import ValidationError

from varco_core.dto.base import UpdateOperation
from varco_core.dto.factory import DTOSet, generate_dtos
from varco_core.dto.pagination import (
    PageCursor,
    SortCursorField,
    paged_response,
)
from varco_core.meta import FieldHint, PKStrategy, PrimaryKey, pk_field
from varco_core.model import (
    AuditedDomainModel,
    DomainModel,
    TenantAuditedDomainModel,
    VersionedDomainModel,
)
from varco_core.query.params import QueryParams
from varco_core.query.type import SortField, SortOrder
from varco_core.registry import DomainModelRegistry, register


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clean_registry():
    """
    Prevent cross-test registry pollution.

    DomainModelRegistry is a class-level singleton — any test that uses
    @register would otherwise bleed its classes into subsequent tests.
    """
    DomainModelRegistry.clear()
    yield
    DomainModelRegistry.clear()


# ── generate_dtos — error handling ────────────────────────────────────────────


def test_generate_dtos_raises_on_non_dataclass():
    """
    generate_dtos() must reject classes that are not dataclasses.

    Note: DomainModel subclasses without @dataclass still pass is_dataclass()
    because they inherit __dataclass_fields__ from the base. We use a completely
    unrelated plain class to test the TypeError path.
    """

    class _PlainClass:
        pass

    with pytest.raises(TypeError, match="@dataclass"):
        generate_dtos(_PlainClass)


# ── DTOSet structure ──────────────────────────────────────────────────────────


def test_generate_dtos_returns_dto_set():
    """generate_dtos() returns a DTOSet NamedTuple with .create/.read/.update."""

    @dataclass
    class _Widget(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        name: str

        class Meta:
            table = "widgets_a"

    result = generate_dtos(_Widget)

    assert isinstance(result, DTOSet)
    # All three slots must be Pydantic model classes
    assert result.create is not None
    assert result.read is not None
    assert result.update is not None


def test_dto_class_names_follow_convention():
    """Generated class names are {DomainClass}CreateDTO, ReadDTO, UpdateDTO."""

    @dataclass
    class _Article(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str

        class Meta:
            table = "articles_b"

    dtos = generate_dtos(_Article)

    assert dtos.create.__name__ == "_ArticleCreateDTO"
    assert dtos.read.__name__ == "_ArticleReadDTO"
    assert dtos.update.__name__ == "_ArticleUpdateDTO"


# ── CreateDTO field rules ─────────────────────────────────────────────────────


def test_create_dto_excludes_pk():
    """
    pk is always excluded from CreateDTO — callers must never submit a raw PK
    in an HTTP payload; the service layer generates or validates it.
    """

    @dataclass
    class _Post(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        body: str

        class Meta:
            table = "posts_c"

    CreateDTO = generate_dtos(_Post).create

    assert "pk" not in CreateDTO.model_fields


def test_create_dto_includes_required_business_fields():
    """Required business fields (no default) appear in CreateDTO."""

    @dataclass
    class _Post(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str
        body: str

        class Meta:
            table = "posts_d"

    CreateDTO = generate_dtos(_Post).create

    # Constructing with all required fields works
    dto = CreateDTO(title="Hello", body="World")
    assert dto.title == "Hello"
    assert dto.body == "World"


def test_create_dto_required_field_raises_on_missing():
    """A required field with no default raises ValidationError when absent."""

    @dataclass
    class _Product(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        name: str

        class Meta:
            table = "products_e"

    CreateDTO = generate_dtos(_Product).create

    with pytest.raises(ValidationError):
        CreateDTO()  # name missing


def test_create_dto_preserves_concrete_defaults():
    """Concrete defaults (e.g. published: bool = False) carry over to CreateDTO."""

    @dataclass
    class _Article(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str
        published: bool = False

        class Meta:
            table = "articles_f"

    CreateDTO = generate_dtos(_Article).create

    dto = CreateDTO(title="Draft")
    assert dto.published is False


def test_create_dto_optional_field_defaults_to_none():
    """
    Fields typed as T | None with no explicit default get None in CreateDTO.
    This is the conventional Python behaviour for optional annotations.
    """

    @dataclass
    class _Comment(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        body: str
        parent_id: UUID | None = None  # optional FK

        class Meta:
            table = "comments_g"

    CreateDTO = generate_dtos(_Comment).create

    dto = CreateDTO(body="Great post")
    assert dto.parent_id is None


def test_create_dto_excludes_init_false_fields():
    """
    AuditedDomainModel fields (created_at, updated_at) have init=False —
    they are mapper-managed and must not appear in CreateDTO.
    """

    @dataclass
    class _Event(AuditedDomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        name: str

        class Meta:
            table = "events_h"

    CreateDTO = generate_dtos(_Event).create

    assert "created_at" not in CreateDTO.model_fields
    assert "updated_at" not in CreateDTO.model_fields


def test_create_dto_strips_annotated_hints():
    """
    FieldHint / PrimaryKey wrappers inside Annotated are stripped.
    Pydantic receives the bare type (str, int, etc.) — not the annotated form.
    """

    @dataclass
    class _User(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        email: Annotated[str, FieldHint(unique=True, max_length=255)]

        class Meta:
            table = "users_i"

    CreateDTO = generate_dtos(_User).create

    # email must be present and accept a plain string
    dto = CreateDTO(email="alice@example.com")
    assert dto.email == "alice@example.com"


def test_create_dto_default_factory_field():
    """
    Fields with default_factory (e.g. list) are wrapped in Pydantic Field()
    so instances don't share the same mutable default object.
    """
    from dataclasses import field as dc_field

    @dataclass
    class _Collection(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        tags: list[str] = dc_field(default_factory=list)

        class Meta:
            table = "collections_j"

    CreateDTO = generate_dtos(_Collection).create

    dto_a = CreateDTO()
    dto_b = CreateDTO()
    # Mutable defaults must NOT be shared across instances
    dto_a.tags.append("hello")
    assert "hello" not in dto_b.tags


# ── UpdateDTO field rules ─────────────────────────────────────────────────────


def test_update_dto_all_fields_optional():
    """
    Every business field becomes T | None = None in UpdateDTO (sparse update).
    Callers send only the fields they want to change.
    """

    @dataclass
    class _Article(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str
        published: bool = False

        class Meta:
            table = "articles_k"

    UpdateDTO = generate_dtos(_Article).update

    # Construction with no arguments must succeed — everything defaults to None
    dto = UpdateDTO()
    assert dto.title is None
    assert dto.published is None


def test_update_dto_inherits_op_field():
    """UpdateDTO inherits the op: UpdateOperation field from the base class."""

    @dataclass
    class _Item(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        name: str

        class Meta:
            table = "items_l"

    UpdateDTO = generate_dtos(_Item).update

    dto = UpdateDTO()
    # Default op is REPLACE per UpdateDTO base definition
    assert dto.op == UpdateOperation.REPLACE


def test_update_dto_op_can_be_overridden():
    """op can be set to any valid UpdateOperation value."""

    @dataclass
    class _Item(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        tags: list[str] = None  # type: ignore[assignment]

        class Meta:
            table = "items_m"

    UpdateDTO = generate_dtos(_Item).update

    dto = UpdateDTO(op=UpdateOperation.EXTEND, tags=["new"])
    assert dto.op == UpdateOperation.EXTEND


def test_update_dto_excludes_pk():
    """pk is excluded from UpdateDTO — same rule as CreateDTO."""

    @dataclass
    class _Article(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str

        class Meta:
            table = "articles_n"

    UpdateDTO = generate_dtos(_Article).update

    assert "pk" not in UpdateDTO.model_fields


# ── ReadDTO field rules ───────────────────────────────────────────────────────


def test_read_dto_has_pk_with_concrete_type():
    """
    ReadDTO overrides the base's broad pk: Any with the exact domain PK type.
    This gives type checkers and Pydantic the correct annotation.
    """

    @dataclass
    class _Post(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str

        class Meta:
            table = "posts_o"

    ReadDTO = generate_dtos(_Post).read

    pk_val = UUID("12345678-1234-5678-1234-567812345678")
    dto = ReadDTO(pk=pk_val, title="Hello", created_at=_NOW, updated_at=_NOW)
    assert dto.pk == pk_val


def test_read_dto_does_not_redeclare_created_at_updated_at():
    """
    created_at / updated_at come from the ReadDTO base class and must NOT
    be re-declared — Pydantic raises on duplicate field definitions.
    If the generated class instantiates cleanly, there is no duplicate.
    """

    @dataclass
    class _Post(AuditedDomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str

        class Meta:
            table = "posts_p"

    ReadDTO = generate_dtos(_Post).read

    # If Pydantic raised on duplicate fields, this line would have already failed
    dto = ReadDTO(
        pk=UUID("12345678-1234-5678-1234-567812345678"),
        title="t",
        created_at=_NOW,
        updated_at=_NOW,
    )
    assert dto.title == "t"


def test_read_dto_includes_versioned_fields():
    """
    definition_version and row_version (from VersionedDomainModel) have
    init=False so they are excluded from CreateDTO/UpdateDTO, but they
    ARE included in ReadDTO (the client should see them).
    """

    @dataclass
    class _Doc(VersionedDomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        content: str

        class Meta:
            table = "docs_q"

    dtos = generate_dtos(_Doc)

    # Excluded from create/update (init=False)
    assert "definition_version" not in dtos.create.model_fields
    assert "row_version" not in dtos.create.model_fields

    # Included in read (all non-private, non-base fields appear)
    assert "definition_version" in dtos.read.model_fields
    assert "row_version" in dtos.read.model_fields


def test_read_dto_includes_tenant_id():
    """
    tenant_id from TenantMixin has init=True, so it appears in CreateDTO /
    UpdateDTO AND ReadDTO — callers can see which tenant owns the record.

    title receives a default so that Python's dataclass field-ordering rule
    is satisfied (non-default fields cannot follow default fields when both
    come from the same dataclass hierarchy).
    """

    @dataclass
    class _TenantPost(TenantAuditedDomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str  # no default needed — tenant_id is kw_only in TenantMixin

        class Meta:
            table = "tenant_posts_r"

    dtos = generate_dtos(_TenantPost)

    # tenant_id must be visible in read
    assert "tenant_id" in dtos.read.model_fields
    # and also available in create/update (caller or service stamps it)
    assert "tenant_id" in dtos.create.model_fields


# ── Meta.auto_dto = True ──────────────────────────────────────────────────────


def test_auto_dto_stamps_dtos_onto_class():
    """
    When Meta.auto_dto = True, @register automatically generates DTOs
    and stamps them as domain_cls.DTOs.
    """

    @register
    @dataclass
    class _AutoArticle(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str

        class Meta:
            table = "auto_articles_s"
            auto_dto = True

    assert hasattr(_AutoArticle, "DTOs")
    assert isinstance(_AutoArticle.DTOs, DTOSet)


def test_auto_dto_false_does_not_stamp():
    """When auto_dto is False (or absent), no .DTOs attribute is set."""

    @register
    @dataclass
    class _ManualArticle(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        title: str

        class Meta:
            table = "manual_articles_t"
            # auto_dto not set — defaults to absent / False

    assert not hasattr(_ManualArticle, "DTOs")


def test_auto_dto_dtos_accessible_via_class():
    """DTOs attached by @register are accessible via .DTOs.create/.read/.update."""

    @register
    @dataclass
    class _Widget(DomainModel):
        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        label: str

        class Meta:
            table = "widgets_u"
            auto_dto = True

    # All three DTO classes must be reachable and instantiable
    create_dto = _Widget.DTOs.create(label="A")
    assert create_dto.label == "A"

    pk_val = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    read_dto = _Widget.DTOs.read(pk=pk_val, label="B", created_at=_NOW, updated_at=_NOW)
    assert read_dto.label == "B"

    update_dto = _Widget.DTOs.update(label="C")
    assert update_dto.label == "C"


# ── Shared ReadDTO fixture for pagination tests ───────────────────────────────

# Pydantic v2 validates list[T] contents at runtime — results must be real
# ReadDTO instances, not plain Python objects.  A minimal subclass suffices.

_NOW = datetime.now(tz=_tz.utc)

# Use the generated ReadDTO from a real domain model so we get proper
# Pydantic validation — Pydantic v2 validates list[ReadDTO] contents at runtime.


@dataclass
class _PaginationDomain(AuditedDomainModel):
    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
    label: str = ""

    class Meta:
        table = "pagination_domain_zz"


_PaginationDTOs = generate_dtos(_PaginationDomain)
_PaginationReadDTO = _PaginationDTOs.read

_FIXED_PK = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def _make_items(n: int) -> list:
    """Return n ReadDTO instances for use in pagination tests."""
    return [
        _PaginationReadDTO(pk=_FIXED_PK, label=str(i), created_at=_NOW, updated_at=_NOW)
        for i in range(n)
    ]


# ── paged_response — type validation ─────────────────────────────────────────


def test_paged_response_raises_on_non_query_params():
    """
    paged_response() validates params is a QueryParams instance — passing any
    other object raises TypeError with a descriptive message.
    """
    with pytest.raises(TypeError, match="QueryParams"):
        paged_response([], params=object())  # type: ignore[arg-type]


# ── paged_response — with total_count (precise strategy) ─────────────────────


def test_paged_response_next_is_none_on_last_page_with_total_count():
    """
    With total_count provided, next is None when all results are exhausted
    (next_offset == total_count).
    """
    params = QueryParams(limit=10, offset=0)
    results = _make_items(5)  # only 5 results — last page

    response = paged_response(results, params=params, total_count=5)

    assert response.next is None
    assert response.total_count == 5
    assert response.count == 5


def test_paged_response_next_is_set_when_more_results_exist():
    """
    With total_count provided, next is a PageCursor when
    current_offset + count < total_count.
    """
    params = QueryParams(limit=10, offset=0)
    results = _make_items(10)  # full page

    response = paged_response(results, params=params, total_count=25)

    assert response.next is not None
    assert response.next.offset == 10  # next_offset = 0 + 10
    assert response.next.limit == 10


def test_paged_response_next_offset_uses_result_count_not_limit():
    """
    DESIGN: next.offset = current_offset + len(results), NOT + limit.
    This correctly handles a partial last page returned due to row-level filters.
    """
    # limit=10 but only 3 results returned (row-level filter) out of 103 total
    params = QueryParams(limit=10, offset=100)
    results = _make_items(3)

    response = paged_response(results, params=params, total_count=103)

    # next_offset = 100 + 3 = 103, which equals total_count → no next
    assert response.next is None


def test_paged_response_next_carries_sort_directives():
    """
    The PageCursor carries forward the sort directives from QueryParams
    so clients can replicate the exact same ordering on the next page.
    """
    sort = [SortField("created_at", SortOrder.DESC), SortField("pk", SortOrder.ASC)]
    params = QueryParams(limit=5, offset=0, sort=sort)
    results = _make_items(5)

    response = paged_response(results, params=params, total_count=20)

    assert response.next is not None
    assert len(response.next.sort) == 2
    assert response.next.sort[0].field == "created_at"
    assert response.next.sort[0].order == SortOrder.DESC
    assert response.next.sort[1].field == "pk"
    assert response.next.sort[1].order == SortOrder.ASC


def test_paged_response_next_carries_raw_query():
    """
    raw_query is forwarded into PageCursor.query so the client can copy the
    cursor directly into the next HTTP request without reconstructing the filter.
    """
    params = QueryParams(limit=10, offset=0)
    results = _make_items(10)

    response = paged_response(
        results, params=params, total_count=50, raw_query='status = "active"'
    )

    assert response.next is not None
    assert response.next.query == 'status = "active"'


def test_paged_response_next_query_none_when_no_raw_query():
    """When raw_query is omitted, PageCursor.query is None."""
    params = QueryParams(limit=10, offset=0)
    results = _make_items(10)

    response = paged_response(results, params=params, total_count=50)

    assert response.next is not None
    assert response.next.query is None


# ── paged_response — without total_count (heuristic strategy) ─────────────────


def test_paged_response_heuristic_next_set_on_full_page():
    """
    Without total_count, next is set when count == limit (heuristic: full page
    suggests more results may exist beyond this page).
    """
    params = QueryParams(limit=5, offset=0)
    results = _make_items(5)  # exactly limit items

    response = paged_response(results, params=params)  # no total_count

    assert response.next is not None
    assert response.total_count is None


def test_paged_response_heuristic_next_none_on_partial_page():
    """
    Without total_count, next is None when count < limit (partial page → last page).
    """
    params = QueryParams(limit=10, offset=0)
    results = _make_items(3)  # fewer than limit → last page

    response = paged_response(results, params=params)

    assert response.next is None


# ── paged_response — unbounded queries ────────────────────────────────────────


def test_paged_response_no_next_when_limit_is_none():
    """
    Unbounded queries (limit=None) have no meaningful next-page concept —
    next must always be None regardless of result count or total_count.
    """
    params = QueryParams(limit=None, offset=0)
    results = _make_items(10)

    response = paged_response(results, params=params, total_count=500)

    assert response.next is None


# ── paged_response — empty results ────────────────────────────────────────────


def test_paged_response_empty_results_with_total_count_zero():
    """An empty result set with total_count=0 is a valid empty response."""
    params = QueryParams(limit=10, offset=0)

    response = paged_response([], params=params, total_count=0)

    assert response.count == 0
    assert response.results == []
    assert response.next is None
    assert response.total_count == 0


# ── PagedReadDTO structure ────────────────────────────────────────────────────


def test_paged_read_dto_count_reflects_results_length():
    """count is always len(results), not the limit."""
    params = QueryParams(limit=10, offset=0)
    results = _make_items(7)

    response = paged_response(results, params=params, total_count=7)

    assert response.count == 7


def test_paged_read_dto_results_are_preserved():
    """The results list is passed through unchanged."""
    params = QueryParams(limit=3, offset=0)
    items = _make_items(3)

    response = paged_response(items, params=params, total_count=3)

    assert response.results == items


# ── PageCursor validation ─────────────────────────────────────────────────────


def test_page_cursor_requires_positive_limit():
    """PageCursor rejects limit < 1 — ge=1 constraint from Pydantic Field."""
    with pytest.raises(ValidationError):
        PageCursor(limit=0, offset=0)


def test_page_cursor_requires_non_negative_offset():
    """PageCursor rejects offset < 0 — ge=0 constraint from Pydantic Field."""
    with pytest.raises(ValidationError):
        PageCursor(limit=10, offset=-1)


def test_page_cursor_sort_defaults_to_empty_list():
    """sort defaults to an empty list when not supplied."""
    cursor = PageCursor(limit=10, offset=20)
    assert cursor.sort == []


def test_page_cursor_query_defaults_to_none():
    """query defaults to None when not supplied."""
    cursor = PageCursor(limit=10, offset=0)
    assert cursor.query is None


# ── SortCursorField serialization ─────────────────────────────────────────────


def test_sort_cursor_field_serializes_order_as_string():
    """
    SortOrder is a StrEnum — serialized as "ASC" / "DESC", not the enum member.
    Clients receive a plain string they can POST back unchanged.
    """
    field = SortCursorField(field="created_at", order=SortOrder.DESC)

    serialized = field.model_dump()

    assert serialized["field"] == "created_at"
    assert serialized["order"] == "DESC"


def test_sort_cursor_field_round_trips_json():
    """SortCursorField can be reconstructed from its own JSON output."""
    original = SortCursorField(field="name", order=SortOrder.ASC)
    json_str = original.model_dump_json()

    reconstructed = SortCursorField.model_validate_json(json_str)

    assert reconstructed.field == original.field
    assert reconstructed.order == original.order
