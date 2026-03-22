"""
Tests for DomainModel, AuditedDomainModel, VersionedDomainModel, and cast_raw().

Covers:
- is_persisted() — False before, True after backing ORM set
- raw() — happy path and RuntimeError guard
- cast_raw() — typed access, TypeError on wrong ORM type
- AuditedDomainModel and VersionedDomainModel default field values

Thread safety:  N/A (unit tests, no concurrency)
Async safety:   N/A (all synchronous)
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from dataclasses import fields as dc_fields

from varco_core.model import (
    AuditedDomainModel,
    DomainModel,
    TenantAuditedDomainModel,
    TenantDomainModel,
    TenantMixin,
    TenantVersionedDomainModel,
    VersionedDomainModel,
    cast_raw,
)


# ── Minimal concrete subclasses for testing ───────────────────────────────────


@dataclass
class _Widget(DomainModel):
    """Minimal concrete DomainModel for testing base-class behaviour."""

    name: str = ""


@dataclass
class _Audited(AuditedDomainModel):
    """Concrete AuditedDomainModel subclass for field-presence tests."""

    title: str = ""


@dataclass
class _Versioned(VersionedDomainModel):
    """Concrete VersionedDomainModel subclass for field-presence tests."""

    label: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────


class _FakeOrm:
    """Stand-in ORM class used to test cast_raw() type checking."""


class _WrongOrm:
    """Different ORM class — used to trigger cast_raw() TypeError."""


# ── DomainModel.is_persisted() ────────────────────────────────────────────────


def test_is_persisted_false_on_fresh_entity() -> None:
    """Freshly constructed entity has no backing ORM → is_persisted is False."""
    widget = _Widget(name="gear")
    assert widget.is_persisted() is False


def test_is_persisted_false_when_raw_orm_is_none() -> None:
    """Explicitly setting _raw_orm to None keeps is_persisted False."""
    widget = _Widget(name="gear")
    # _raw_orm is None by default; confirm the field exists and is None
    assert widget._raw_orm is None
    assert widget.is_persisted() is False


def test_is_persisted_true_after_backing_orm_set() -> None:
    """is_persisted returns True as soon as _raw_orm is assigned."""
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()
    assert widget.is_persisted() is True


def test_is_persisted_false_after_clearing_raw_orm() -> None:
    """
    Clearing _raw_orm resets is_persisted to False.

    Edge case: unlikely in production but a valid state transition
    to document — is_persisted is derived purely from _raw_orm.
    """
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()
    assert widget.is_persisted() is True

    widget._raw_orm = None
    assert widget.is_persisted() is False


# ── DomainModel.raw() ─────────────────────────────────────────────────────────


def test_raw_returns_backing_orm_object() -> None:
    """raw() returns the backing ORM object when the entity is persisted."""
    orm = _FakeOrm()
    widget = _Widget(name="gear")
    widget._raw_orm = orm

    assert widget.raw() is orm


def test_raw_raises_runtime_error_on_fresh_entity() -> None:
    """raw() raises RuntimeError when called on an unpersisted entity."""
    widget = _Widget(name="gear")

    with pytest.raises(RuntimeError, match="raw\\(\\) called before entity was loaded"):
        widget.raw()


def test_raw_error_message_includes_class_name() -> None:
    """RuntimeError message includes the subclass name for easy debugging."""
    widget = _Widget(name="gear")

    with pytest.raises(RuntimeError, match="_Widget"):
        widget.raw()


# ── cast_raw() ────────────────────────────────────────────────────────────────


def test_cast_raw_returns_typed_orm_object() -> None:
    """cast_raw returns the raw ORM object typed as orm_type."""
    orm = _FakeOrm()
    widget = _Widget(name="gear")
    widget._raw_orm = orm

    result = cast_raw(widget, _FakeOrm)
    assert result is orm


def test_cast_raw_raises_type_error_for_wrong_orm_type() -> None:
    """
    cast_raw raises TypeError when the backing object is not an instance
    of the requested ORM type.

    Edge case: caller accidentally passes the wrong ORM class — should get
    a clear error rather than a silent None or AttributeError later.
    """
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()  # backed by _FakeOrm, not _WrongOrm

    with pytest.raises(TypeError, match="_WrongOrm"):
        cast_raw(widget, _WrongOrm)


def test_cast_raw_raises_runtime_error_when_not_persisted() -> None:
    """cast_raw propagates RuntimeError from raw() when entity is unpersisted."""
    widget = _Widget(name="gear")

    with pytest.raises(RuntimeError):
        cast_raw(widget, _FakeOrm)


def test_cast_raw_error_message_names_expected_and_actual_types() -> None:
    """TypeError message names both expected and actual ORM types."""
    widget = _Widget(name="gear")
    widget._raw_orm = _FakeOrm()

    with pytest.raises(TypeError) as exc_info:
        cast_raw(widget, _WrongOrm)

    msg = str(exc_info.value)
    # Must name what was requested and what was found
    assert "_WrongOrm" in msg
    assert "_FakeOrm" in msg


# ── AuditedDomainModel field defaults ────────────────────────────────────────


def test_audited_domain_model_has_none_timestamps_on_construction() -> None:
    """AuditedDomainModel has created_at and updated_at defaulted to None."""
    entity = _Audited(title="test")
    # Both timestamps are None until a repository operation populates them
    assert entity.created_at is None
    assert entity.updated_at is None


def test_audited_domain_model_inherits_pk_and_raw_orm() -> None:
    """AuditedDomainModel still has pk and _raw_orm from DomainModel."""
    entity = _Audited(title="test")
    assert entity.pk is None
    assert entity._raw_orm is None
    assert entity.is_persisted() is False


def test_audited_domain_model_is_persisted_tracks_raw_orm() -> None:
    """AuditedDomainModel.is_persisted() delegates to _raw_orm check."""
    entity = _Audited(title="test")
    entity._raw_orm = MagicMock()
    assert entity.is_persisted() is True


# ── VersionedDomainModel field defaults ──────────────────────────────────────


def test_versioned_domain_model_has_default_field_values() -> None:
    """VersionedDomainModel starts with known sentinel values for system fields."""
    entity = _Versioned(label="v")
    # definition_version starts at 1 (schema not yet migrated)
    assert entity.definition_version == 1
    # row_version starts at 0 (not yet persisted — row_version 1 is set on INSERT)
    assert entity.row_version == 0


def test_versioned_domain_model_inherits_audit_timestamps() -> None:
    """VersionedDomainModel inherits created_at / updated_at from AuditedDomainModel."""
    entity = _Versioned(label="v")
    assert entity.created_at is None
    assert entity.updated_at is None


def test_versioned_domain_model_is_persisted_tracks_raw_orm() -> None:
    """VersionedDomainModel.is_persisted() delegates to _raw_orm check."""
    entity = _Versioned(label="v")
    assert entity.is_persisted() is False

    entity._raw_orm = MagicMock()
    assert entity.is_persisted() is True


# ── DomainModel pk field ──────────────────────────────────────────────────────


def test_pk_defaults_to_none() -> None:
    """pk is None on a freshly constructed entity — not in constructor."""
    widget = _Widget(name="gear")
    assert widget.pk is None


def test_pk_can_be_set_directly() -> None:
    """
    pk can be set directly (needed by STR_ASSIGNED / CUSTOM strategies).

    Not using pk_field(init=True) here — just verifying field is mutable.
    """
    widget = _Widget(name="gear")
    widget.pk = "custom-key"
    assert widget.pk == "custom-key"


# ── Tenant model concrete subclasses for testing ──────────────────────────────


@dataclass
class _TenantWidget(TenantDomainModel):
    """Minimal concrete TenantDomainModel — no timestamps, no versioning."""

    name: str = ""


@dataclass
class _TenantAudited(TenantAuditedDomainModel):
    """Concrete TenantAuditedDomainModel — timestamps + tenant_id."""

    title: str = ""


@dataclass
class _TenantVersioned(TenantVersionedDomainModel):
    """Concrete TenantVersionedDomainModel — all system fields + tenant_id."""

    body: str = ""


# ── TenantMixin ───────────────────────────────────────────────────────────────


def test_tenant_mixin_provides_tenant_id_field() -> None:
    """TenantMixin declares a tenant_id dataclass field."""
    field_names = {f.name for f in dc_fields(TenantMixin)}
    assert "tenant_id" in field_names


def test_tenant_mixin_tenant_id_defaults_to_empty_string() -> None:
    """
    tenant_id defaults to "" — the empty string is the safe construction-time
    sentinel; TenantAwareService.create() stamps the real value before save().
    """
    mixin = TenantMixin()
    assert mixin.tenant_id == ""


def test_tenant_mixin_tenant_id_is_init_parameter() -> None:
    """
    tenant_id is an init parameter (init=True) — required so that
    dataclasses.replace(entity, tenant_id=tid) works without a TypeError.
    """
    field_map = {f.name: f for f in dc_fields(TenantMixin)}
    assert field_map["tenant_id"].init is True


def test_tenant_mixin_tenant_id_participates_in_comparison() -> None:
    """
    tenant_id has compare=True — two TenantMixin instances with different
    tenant_ids are not equal (they represent rows from different tenants).
    """
    a = TenantMixin()
    a.tenant_id = "acme"
    b = TenantMixin()
    b.tenant_id = "globex"
    assert a != b


def test_tenant_mixin_tenant_id_has_field_hint_annotation() -> None:
    """
    The tenant_id annotation carries a FieldHint with index=True and
    nullable=False — verified via the type annotation on TenantMixin.
    """
    import typing

    from varco_core.meta import FieldHint

    hints = typing.get_type_hints(TenantMixin, include_extras=True)
    annotated_args = typing.get_args(hints["tenant_id"])
    # Annotated[str, FieldHint(...)] — second arg is the FieldHint
    field_hint = next((a for a in annotated_args if isinstance(a, FieldHint)), None)
    assert field_hint is not None, "FieldHint missing from tenant_id annotation"
    assert field_hint.index is True
    assert field_hint.nullable is False


# ── TenantDomainModel ─────────────────────────────────────────────────────────


def test_tenant_domain_model_tenant_id_defaults_to_empty_string() -> None:
    """
    TenantDomainModel.tenant_id defaults to "" — TenantAwareService.create()
    always overwrites it before save() via dataclasses.replace.
    """
    entity = _TenantWidget(name="widget")
    assert entity.tenant_id == ""


def test_tenant_domain_model_tenant_id_accepts_constructor_value() -> None:
    """
    tenant_id is an init parameter — can be passed at construction time.
    This is required for dataclasses.replace(entity, tenant_id=tid) to work.
    """
    entity = _TenantWidget(name="widget", tenant_id="acme")
    assert entity.tenant_id == "acme"


def test_tenant_domain_model_replace_stamps_tenant_without_touching_other_fields() -> (
    None
):
    """
    dataclasses.replace(entity, tenant_id=tid) leaves all other fields intact.
    This is the exact mechanism TenantAwareService.create() uses after assembly.
    """
    from dataclasses import replace

    entity = _TenantWidget(name="important-name")
    stamped = replace(entity, tenant_id="acme")

    assert stamped.tenant_id == "acme"
    assert stamped.name == "important-name"  # other fields preserved
    assert entity.tenant_id == ""  # original unchanged


def test_tenant_domain_model_inherits_pk_raw_orm_and_is_persisted() -> None:
    """TenantDomainModel inherits pk, _raw_orm, and is_persisted() from DomainModel."""
    entity = _TenantWidget(name="widget")
    assert entity.pk is None
    assert entity._raw_orm is None
    assert entity.is_persisted() is False


def test_tenant_domain_model_is_subclass_of_domain_model_and_tenant_mixin() -> None:
    """TenantDomainModel satisfies isinstance checks for both parent types."""
    entity = _TenantWidget(name="widget")
    assert isinstance(entity, DomainModel)
    assert isinstance(entity, TenantMixin)


def test_tenant_domain_model_equality_differs_across_tenants() -> None:
    """
    Two entities with identical business fields but different tenant_ids are NOT
    equal — tenant_id participates in comparison (compare=True) because rows
    from different tenants are genuinely distinct.
    """
    a = _TenantWidget(name="widget", tenant_id="acme")
    b = _TenantWidget(name="widget", tenant_id="globex")
    assert a != b


def test_tenant_domain_model_equality_same_tenant_and_fields() -> None:
    """Two entities with identical fields and the same tenant_id are equal."""
    a = _TenantWidget(name="widget", tenant_id="acme")
    b = _TenantWidget(name="widget", tenant_id="acme")
    assert a == b


def test_tenant_domain_model_field_order_no_type_error() -> None:
    """
    TenantMixin places tenant_id AFTER all init=False base fields — constructing
    a subclass with only keyword arguments must never raise a TypeError due to
    non-default arguments following default arguments.
    """
    # If field ordering were wrong, @dataclass would raise TypeError at class
    # definition time and this import / construction would already have failed.
    entity = _TenantWidget(name="gear", tenant_id="acme")
    assert entity.name == "gear"
    assert entity.tenant_id == "acme"


# ── TenantAuditedDomainModel ──────────────────────────────────────────────────


def test_tenant_audited_domain_model_has_all_expected_fields() -> None:
    """
    TenantAuditedDomainModel exposes all five system fields:
    pk, _raw_orm (from DomainModel), created_at, updated_at (from
    AuditedDomainModel), and tenant_id (from TenantMixin).
    """
    entity = _TenantAudited(title="post")
    assert entity.pk is None
    assert entity._raw_orm is None
    assert entity.created_at is None
    assert entity.updated_at is None
    assert entity.tenant_id == ""


def test_tenant_audited_domain_model_tenant_id_init_parameter() -> None:
    """tenant_id can be passed to the TenantAuditedDomainModel constructor."""
    entity = _TenantAudited(title="post", tenant_id="acme")
    assert entity.tenant_id == "acme"


def test_tenant_audited_domain_model_replace_stamps_tenant_and_preserves_init_fields() -> (
    None
):
    """
    dataclasses.replace(entity, tenant_id=tid) sets tenant_id and copies all
    init=True fields from the original.  init=False fields (created_at,
    updated_at, _raw_orm) are reset to their defaults — this is standard
    Python dataclass behaviour, not a framework quirk.

    For TenantAwareService.create() this is correct: the entity from
    to_domain() is freshly assembled (timestamps=None) so resetting them
    back to None is a no-op.  For update(), the assembler's apply_update()
    is responsible for copying any fields it cares about explicitly.
    """
    from dataclasses import replace

    entity = _TenantAudited(title="important-title")
    stamped = replace(entity, tenant_id="acme")

    # tenant_id stamped correctly
    assert stamped.tenant_id == "acme"
    # init=True business field preserved
    assert stamped.title == "important-title"
    # init=False system fields reset to defaults — documented dataclass behaviour
    assert stamped.created_at is None
    assert stamped.updated_at is None
    # original entity untouched
    assert entity.tenant_id == ""


def test_tenant_audited_domain_model_is_subclass_of_audited_and_tenant_mixin() -> None:
    """TenantAuditedDomainModel satisfies isinstance for all three ancestors."""
    entity = _TenantAudited(title="post")
    assert isinstance(entity, DomainModel)
    assert isinstance(entity, AuditedDomainModel)
    assert isinstance(entity, TenantMixin)


def test_tenant_audited_domain_model_inherits_is_persisted() -> None:
    """TenantAuditedDomainModel.is_persisted() delegates to _raw_orm."""
    entity = _TenantAudited(title="post")
    assert entity.is_persisted() is False

    entity._raw_orm = MagicMock()
    assert entity.is_persisted() is True


def test_tenant_audited_domain_model_equality_differs_across_tenants() -> None:
    """tenant_id participates in equality for TenantAuditedDomainModel too."""
    a = _TenantAudited(title="post", tenant_id="acme")
    b = _TenantAudited(title="post", tenant_id="globex")
    assert a != b


# ── TenantVersionedDomainModel ────────────────────────────────────────────────


def test_tenant_versioned_domain_model_has_all_expected_fields() -> None:
    """
    TenantVersionedDomainModel exposes all seven system fields:
    pk, _raw_orm (DomainModel), created_at, updated_at (AuditedDomainModel),
    definition_version, row_version (VersionedDomainModel), tenant_id (TenantMixin).
    """
    entity = _TenantVersioned(body="hello")
    assert entity.pk is None
    assert entity._raw_orm is None
    assert entity.created_at is None
    assert entity.updated_at is None
    assert entity.definition_version == 1  # VersionedDomainModel default
    assert entity.row_version == 0  # VersionedDomainModel default
    assert entity.tenant_id == ""  # TenantMixin default


def test_tenant_versioned_domain_model_tenant_id_init_parameter() -> None:
    """tenant_id can be passed at construction time."""
    entity = _TenantVersioned(body="doc", tenant_id="acme")
    assert entity.tenant_id == "acme"


def test_tenant_versioned_domain_model_replace_stamps_tenant_and_resets_init_false_fields() -> (
    None
):
    """
    dataclasses.replace(entity, tenant_id=tid) sets tenant_id and copies all
    init=True fields.  definition_version and row_version are init=False so
    they reset to their class defaults (1 and 0 respectively) — same as
    created_at / updated_at on TenantAuditedDomainModel.

    Tenant stamping and optimistic-lock counters are independent concerns:
    TenantAwareService.create() stamps tenant_id on a freshly assembled entity
    (where version fields are already at defaults), so the reset is a no-op
    for the INSERT path.
    """
    from dataclasses import replace

    entity = _TenantVersioned(body="important-body")
    stamped = replace(entity, tenant_id="acme")

    # tenant_id stamped correctly
    assert stamped.tenant_id == "acme"
    # init=True business field preserved
    assert stamped.body == "important-body"
    # init=False version fields reset to class defaults — documented behaviour
    assert stamped.definition_version == 1
    assert stamped.row_version == 0
    # original entity untouched
    assert entity.tenant_id == ""


def test_tenant_versioned_domain_model_is_subclass_of_all_ancestors() -> None:
    """TenantVersionedDomainModel satisfies isinstance for its full ancestry."""
    entity = _TenantVersioned(body="doc")
    assert isinstance(entity, DomainModel)
    assert isinstance(entity, AuditedDomainModel)
    assert isinstance(entity, VersionedDomainModel)
    assert isinstance(entity, TenantMixin)


def test_tenant_versioned_domain_model_inherits_is_persisted() -> None:
    """is_persisted() works correctly on TenantVersionedDomainModel."""
    entity = _TenantVersioned(body="doc")
    assert entity.is_persisted() is False

    entity._raw_orm = MagicMock()
    assert entity.is_persisted() is True


def test_tenant_versioned_domain_model_equality_differs_across_tenants() -> None:
    """tenant_id participates in equality for TenantVersionedDomainModel."""
    a = _TenantVersioned(body="doc", tenant_id="acme")
    b = _TenantVersioned(body="doc", tenant_id="globex")
    assert a != b


def test_tenant_versioned_domain_model_field_order_no_type_error() -> None:
    """
    With seven system fields in the MRO, constructing TenantVersionedDomainModel
    must not raise TypeError — TenantMixin's tenant_id (init=True, default="")
    trails all init=False system fields so the dataclass ordering rules are met.
    """
    entity = _TenantVersioned(body="content", tenant_id="acme")
    assert entity.body == "content"
    assert entity.tenant_id == "acme"


# ── Custom TenantMixin composition ────────────────────────────────────────────


def test_custom_mixin_composition_user_can_combine_tenant_mixin_with_any_base() -> None:
    """
    Advanced users can compose TenantMixin with any DomainModel subclass that
    is not one of the three provided concrete bases.  Verify the mixin attaches
    tenant_id to the composed class correctly.
    """

    @dataclass
    class _CustomBase(DomainModel):
        score: int = 0

    @dataclass
    class _TenantCustom(TenantMixin, _CustomBase):
        pass

    entity = _TenantCustom(score=42, tenant_id="custom_tenant")
    assert entity.score == 42
    assert entity.tenant_id == "custom_tenant"
    assert isinstance(entity, DomainModel)
    assert isinstance(entity, TenantMixin)
