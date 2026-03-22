"""
Tests for BeanieRepositoryProvider.

beanie.init_beanie and the BeanieModelFactory are mocked so no MongoDB
connection is needed.

Coverage:
- register():        builds ORM doc + mapper for each class, idempotent
- get_repository():  returns an AsyncBeanieRepository for registered class
- make_uow():        returns a BeanieUnitOfWork with correct repo factories
- init():            calls beanie.init_beanie() with all registered documents
- _get_built():      raises KeyError for unregistered class

Thread safety:  N/A (unit tests)
Async safety:   ✅ init() is async — tested with AsyncMock
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from varco_core.model import DomainModel
from varco_beanie.provider import BeanieRepositoryProvider
from varco_beanie.repository import AsyncBeanieRepository
from varco_beanie.uow import BeanieUnitOfWork


# ── Test domain models ────────────────────────────────────────────────────────


@dataclass
class _User(DomainModel):
    name: str = ""


@dataclass
class _Post(DomainModel):
    title: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_provider(
    *,
    transactional: bool = False,
    factory_build_return: tuple | None = None,
) -> BeanieRepositoryProvider:
    """
    Build a BeanieRepositoryProvider with a mocked BeanieModelFactory.

    We patch the factory's build() method so that no real Beanie Document
    generation happens — each call returns a (doc_class, mapper) tuple.

    Args:
        transactional:        Passed through to the provider.
        factory_build_return: Optional fixed return value for factory.build().
                              Defaults to a fresh (MagicMock, MagicMock) pair.

    Returns:
        A BeanieRepositoryProvider ready for unit testing.
    """
    mock_mongo_client = MagicMock()

    with patch("varco_beanie.provider.BeanieModelFactory") as mock_factory_cls:
        mock_factory = MagicMock()
        if factory_build_return is None:
            factory_build_return = (MagicMock(), MagicMock())
        mock_factory.build.return_value = factory_build_return
        mock_factory_cls.return_value = mock_factory

        provider = BeanieRepositoryProvider(
            mongo_client=mock_mongo_client,
            db_name="testdb",
            transactional=transactional,
        )
        provider._factory = mock_factory  # keep the mock accessible
    return provider


# ── register() ────────────────────────────────────────────────────────────────


def test_register_builds_orm_class_for_each_entity() -> None:
    """register() calls factory.build() for each domain class passed."""
    provider = _make_provider()
    provider.register(_User, _Post)

    assert provider._factory.build.call_count == 2


def test_register_idempotent_for_same_class() -> None:
    """
    Registering the same class twice only builds it once.

    Edge case: autodiscover() may call register() for already-registered
    classes if the module is scanned more than once.
    """
    provider = _make_provider()
    provider.register(_User)
    provider.register(_User)  # second call — should be a no-op

    assert provider._factory.build.call_count == 1


def test_register_stores_built_tuple_in_internal_cache() -> None:
    """The (doc_class, mapper) tuple is stored under the entity class key."""
    doc_cls = MagicMock()
    mapper = MagicMock()
    provider = _make_provider(factory_build_return=(doc_cls, mapper))
    provider.register(_User)

    assert _User in provider._built
    assert provider._built[_User] == (doc_cls, mapper)


# ── get_repository() ──────────────────────────────────────────────────────────


def test_get_repository_returns_async_beanie_repository() -> None:
    """get_repository() returns an AsyncBeanieRepository for a registered class."""
    provider = _make_provider()
    provider.register(_User)

    repo = provider.get_repository(_User)

    assert isinstance(repo, AsyncBeanieRepository)


def test_get_repository_raises_key_error_for_unregistered_class() -> None:
    """
    get_repository() raises KeyError when the entity was never registered.

    Edge case: caller forgot to call register() before requesting a repo.
    Error message should be actionable.
    """
    provider = _make_provider()

    with pytest.raises(KeyError, match="_User"):
        provider.get_repository(_User)


# ── make_uow() ────────────────────────────────────────────────────────────────


def test_make_uow_returns_beanie_unit_of_work() -> None:
    """make_uow() returns a BeanieUnitOfWork instance."""
    provider = _make_provider()
    provider.register(_User)

    uow = provider.make_uow()

    assert isinstance(uow, BeanieUnitOfWork)


def test_make_uow_wires_repo_factories_by_attribute_name() -> None:
    """
    make_uow() derives attribute names: User → 'users', Post → 'posts'.

    The attribute name is lowercased class name + 's'.
    """
    provider = _make_provider()
    provider.register(_User, _Post)

    uow = provider.make_uow()

    # Repo factories are keyed by attribute name
    assert "users" in uow._repo_factories
    assert "posts" in uow._repo_factories


def test_make_uow_passes_transactional_flag() -> None:
    """make_uow() forwards the transactional flag to BeanieUnitOfWork."""
    provider = _make_provider(transactional=True)
    provider.register(_User)

    uow = provider.make_uow()

    assert uow._transactional is True


# ── init() ────────────────────────────────────────────────────────────────────


async def test_init_calls_beanie_init_beanie() -> None:
    """init() calls beanie.init_beanie() with the pymongo DB and all documents."""
    doc_cls = MagicMock()
    provider = _make_provider(factory_build_return=(doc_cls, MagicMock()))
    provider.register(_User)

    with (
        patch("beanie.init_beanie", new_callable=AsyncMock) as mock_init,
        patch("varco_beanie.provider.BeanieDocRegistry") as mock_registry,
    ):
        mock_registry.all_documents.return_value = [doc_cls]
        await provider.init()

    mock_init.assert_called_once()


async def test_init_passes_pymongo_db_to_beanie() -> None:
    """init() passes the correct Motor database object to init_beanie()."""
    provider = _make_provider(factory_build_return=(MagicMock(), MagicMock()))

    with (
        patch("beanie.init_beanie", new_callable=AsyncMock) as mock_init,
        patch("varco_beanie.provider.BeanieDocRegistry") as mock_registry,
    ):
        mock_registry.all_documents.return_value = []
        await provider.init()

    # The pymongo client is subscripted with the db_name — verify the db arg was passed
    _, kwargs = mock_init.call_args
    assert "database" in kwargs


# ── _get_built() ──────────────────────────────────────────────────────────────


def test_get_built_raises_key_error_with_helpful_message() -> None:
    """
    _get_built() raises KeyError with a message that names the class and
    tells the caller how to fix the issue.
    """
    provider = _make_provider()

    with pytest.raises(KeyError, match="register"):
        provider._get_built(_User)
