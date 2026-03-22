"""
Tests for the providify DI integration module (varco_beanie.di).

No actual container resolution is performed — instead:
- BeanieSettings is tested as a plain dataclass.
- BeanieModule @Provider methods are called directly on an instance.
- _make_repo_provider is tested as a pure function.
- bind_repositories is tested against a mock container.

Coverage:
- BeanieSettings:          frozen, field defaults, type annotations
- BeanieModule:            repository_provider() creates + inits the provider,
                           query_compiler() returns BeanieQueryCompiler
- _make_repo_provider:     produces a @Provider function with correct return annotation
- bind_repositories:       calls container.provide() for each entity class,
                           raises ValueError when called with no classes

Thread safety:  N/A (unit tests)
Async safety:   ✅ BeanieModule.repository_provider is async — tested with await
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from varco_core.model import DomainModel
from varco_core.providers import RepositoryProvider
from varco_core.repository import AsyncRepository
from varco_beanie.di import (
    BeanieModule,
    BeanieSettings,
    _make_repo_provider,
    bind_repositories,
)
from varco_beanie.query.compiler import BeanieQueryCompiler


# ── Test domain models ────────────────────────────────────────────────────────


@dataclass
class _User(DomainModel):
    name: str = ""


@dataclass
class _Post(DomainModel):
    title: str = ""


# ── BeanieSettings ────────────────────────────────────────────────────────────


def test_beanie_settings_is_frozen() -> None:
    """BeanieSettings is immutable — assigning a field after construction raises."""
    settings = BeanieSettings(motor_client=MagicMock(), db_name="test")

    with pytest.raises((AttributeError, TypeError)):
        settings.db_name = "other"  # type: ignore[misc]


def test_beanie_settings_default_entity_classes_is_empty_tuple() -> None:
    """entity_classes defaults to an empty tuple — no domain classes pre-registered."""
    settings = BeanieSettings(motor_client=MagicMock(), db_name="test")
    assert settings.entity_classes == ()


def test_beanie_settings_default_transactional_is_false() -> None:
    """transactional defaults to False — most deployments use standalone MongoDB."""
    settings = BeanieSettings(motor_client=MagicMock(), db_name="test")
    assert settings.transactional is False


def test_beanie_settings_stores_provided_values() -> None:
    """All provided values are stored as attributes."""
    client = MagicMock()
    settings = BeanieSettings(
        motor_client=client,
        db_name="mydb",
        entity_classes=(_User,),
        transactional=True,
    )

    assert settings.motor_client is client
    assert settings.db_name == "mydb"
    assert settings.entity_classes == (_User,)
    assert settings.transactional is True


# ── BeanieModule.repository_provider() ───────────────────────────────────────


async def test_beanie_module_repository_provider_returns_repository_provider() -> None:
    """repository_provider() returns an object typed as RepositoryProvider."""
    mock_client = MagicMock()
    settings = BeanieSettings(motor_client=mock_client, db_name="testdb")
    module = BeanieModule(settings=settings)

    with (
        patch(
            "varco_beanie.di.BeanieRepositoryProvider", autospec=True
        ) as mock_provider_cls,
        patch("beanie.init_beanie", new_callable=AsyncMock),
        patch("varco_beanie.provider.BeanieDocRegistry") as mock_registry,
    ):
        mock_registry.all_documents.return_value = []
        mock_provider_instance = AsyncMock(spec=RepositoryProvider)
        mock_provider_instance.register = MagicMock()
        mock_provider_instance.init = AsyncMock()
        mock_provider_cls.return_value = mock_provider_instance

        result = await module.repository_provider()

    assert result is mock_provider_instance


async def test_beanie_module_repository_provider_calls_init() -> None:
    """repository_provider() awaits provider.init() to initialise Beanie."""
    mock_client = MagicMock()
    settings = BeanieSettings(motor_client=mock_client, db_name="testdb")
    module = BeanieModule(settings=settings)

    with patch("varco_beanie.di.BeanieRepositoryProvider") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.init = AsyncMock()
        mock_instance.register = MagicMock()
        mock_cls.return_value = mock_instance

        await module.repository_provider()

    mock_instance.init.assert_awaited_once()


async def test_beanie_module_repository_provider_registers_entity_classes() -> None:
    """
    repository_provider() calls provider.register(*entity_classes) when
    entity_classes is non-empty.
    """
    mock_client = MagicMock()
    settings = BeanieSettings(
        motor_client=mock_client,
        db_name="testdb",
        entity_classes=(_User, _Post),
    )
    module = BeanieModule(settings=settings)

    with patch("varco_beanie.di.BeanieRepositoryProvider") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.init = AsyncMock()
        mock_instance.register = MagicMock()
        mock_cls.return_value = mock_instance

        await module.repository_provider()

    mock_instance.register.assert_called_once_with(_User, _Post)


async def test_beanie_module_repository_provider_skips_register_when_no_entities() -> (
    None
):
    """
    repository_provider() does NOT call register() when entity_classes is empty.

    Edge case: user may register entities manually after provider creation.
    """
    mock_client = MagicMock()
    settings = BeanieSettings(motor_client=mock_client, db_name="testdb")
    module = BeanieModule(settings=settings)

    with patch("varco_beanie.di.BeanieRepositoryProvider") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.init = AsyncMock()
        mock_instance.register = MagicMock()
        mock_cls.return_value = mock_instance

        await module.repository_provider()

    mock_instance.register.assert_not_called()


# ── BeanieModule.query_compiler() ────────────────────────────────────────────


def test_beanie_module_query_compiler_returns_beanie_query_compiler() -> None:
    """query_compiler() returns a BeanieQueryCompiler instance."""
    settings = BeanieSettings(motor_client=MagicMock(), db_name="testdb")
    module = BeanieModule(settings=settings)

    result = module.query_compiler()

    assert isinstance(result, BeanieQueryCompiler)


# ── _make_repo_provider() ────────────────────────────────────────────────────


def test_make_repo_provider_produces_callable() -> None:
    """_make_repo_provider returns a callable (the factory function)."""
    fn = _make_repo_provider(_User)
    assert callable(fn)


def test_make_repo_provider_sets_correct_return_annotation() -> None:
    """
    The factory's return annotation is patched to AsyncRepository[_User].

    This is the key mechanism that lets providify register the binding under
    the correct generic alias — without this, all repos would collide on the
    bare AsyncRepository interface.
    """
    fn = _make_repo_provider(_User)
    # The underlying function is the @Provider-decorated version — unwrap it
    # by looking at __wrapped__ or the __annotations__ directly on the fn
    # (Provider stamps metadata but returns the original function object).
    return_annotation = fn.__annotations__.get("return")
    assert return_annotation == AsyncRepository[_User]


def test_make_repo_provider_different_entities_have_different_annotations() -> None:
    """Each call produces a factory with a distinct return annotation."""
    fn_user = _make_repo_provider(_User)
    fn_post = _make_repo_provider(_Post)

    assert fn_user.__annotations__["return"] != fn_post.__annotations__["return"]
    assert fn_user.__annotations__["return"] == AsyncRepository[_User]
    assert fn_post.__annotations__["return"] == AsyncRepository[_Post]


def test_make_repo_provider_function_name_includes_entity_name() -> None:
    """The factory __name__ includes the entity class name for debugging."""
    fn = _make_repo_provider(_User)
    assert "_User" in fn.__name__


# ── bind_repositories() ───────────────────────────────────────────────────────


def test_bind_repositories_calls_provide_for_each_entity() -> None:
    """bind_repositories() calls container.provide() once per entity class."""
    mock_container = MagicMock()
    bind_repositories(mock_container, _User, _Post)

    assert mock_container.provide.call_count == 2


def test_bind_repositories_raises_value_error_with_no_entities() -> None:
    """
    bind_repositories() raises ValueError when called with no entity classes.

    Edge case: calling with an empty list is likely a programming mistake —
    fail fast with a clear message rather than silently registering nothing.
    """
    mock_container = MagicMock()

    with pytest.raises(ValueError, match="requires at least one entity class"):
        bind_repositories(mock_container)


def test_bind_repositories_passes_provider_functions_to_container() -> None:
    """Each call to container.provide() receives a callable factory."""
    mock_container = MagicMock()
    bind_repositories(mock_container, _User)

    provided_fn = mock_container.provide.call_args[0][0]
    assert callable(provided_fn)


def test_bind_repositories_each_factory_has_distinct_annotation() -> None:
    """
    Each factory passed to container.provide() has a distinct return annotation.

    Verifies that the closure correctly captures each entity class — a common
    mistake is late binding where all factories close over the last value of
    the loop variable.
    """
    provided_fns: list = []

    def capture_provide(fn):
        provided_fns.append(fn)

    mock_container = MagicMock()
    mock_container.provide.side_effect = capture_provide
    bind_repositories(mock_container, _User, _Post)

    annotations = [fn.__annotations__["return"] for fn in provided_fns]
    assert AsyncRepository[_User] in annotations
    assert AsyncRepository[_Post] in annotations
    # No duplicates — each entity gets its own distinct binding
    assert len(set(str(a) for a in annotations)) == 2
