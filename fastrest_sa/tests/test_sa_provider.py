"""
Tests for SQLAlchemyRepositoryProvider.

The SAModelFactory is mocked so no real SQLAlchemy model generation happens.
This keeps the tests focused on the provider's coordination logic rather than
re-testing the factory (already covered in test_sa_factory.py).

Coverage:
- register():         builds ORM class + mapper per entity, idempotent
- get_repository():   returns AsyncSQLAlchemyRepository, raises KeyError for unknown
- make_uow():         returns SQLAlchemyUnitOfWork with correct repo factory names,
                      session_factory forwarded correctly
- _get_built():       KeyError error message is actionable

Thread safety:  N/A (unit tests)
Async safety:   N/A (provider API is synchronous)
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import DeclarativeBase

from fastrest_core.model import DomainModel
from fastrest_sa.provider import SQLAlchemyRepositoryProvider
from fastrest_sa.repository import AsyncSQLAlchemyRepository
from fastrest_sa.uow import SQLAlchemyUnitOfWork


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
    build_return: tuple | None = None,
) -> SQLAlchemyRepositoryProvider:
    """
    Build a SQLAlchemyRepositoryProvider with a mocked SAModelFactory.

    DESIGN: patch SAModelFactory at the import location inside provider.py
    so the provider uses the mock throughout its lifetime — including calls
    made lazily inside methods.

    Args:
        build_return: Fixed return value for factory.build().
                      Defaults to a (MagicMock, MagicMock) tuple.

    Returns:
        A provider instance with internal factory replaced by a mock.
    """
    if build_return is None:
        build_return = (MagicMock(), MagicMock())

    with patch("fastrest_sa.provider.SAModelFactory") as mock_factory_cls:
        mock_factory = MagicMock()
        mock_factory.build.return_value = build_return
        mock_factory_cls.return_value = mock_factory

        provider = SQLAlchemyRepositoryProvider(
            base=MagicMock(spec=DeclarativeBase),
            session_factory=MagicMock(),
        )
        # Keep the mock accessible for assertions
        provider._factory = mock_factory

    return provider


# ── register() ────────────────────────────────────────────────────────────────


def test_register_calls_factory_build_per_entity() -> None:
    """register() calls factory.build() once for each domain class."""
    provider = _make_provider()
    provider.register(_User, _Post)

    assert provider._factory.build.call_count == 2


def test_register_single_entity() -> None:
    """register() works with a single entity class."""
    provider = _make_provider()
    provider.register(_User)

    assert provider._factory.build.call_count == 1


def test_register_idempotent_for_same_class() -> None:
    """
    Registering the same class twice only builds once.

    Edge case: a class may appear in both manual register() and autodiscover()
    calls — the guard condition prevents duplicate ORM class generation.
    """
    provider = _make_provider()
    provider.register(_User)
    provider.register(_User)  # second call — no-op

    assert provider._factory.build.call_count == 1


def test_register_stores_tuple_in_built_dict() -> None:
    """The (orm_cls, mapper) tuple is stored under the entity class key."""
    orm_cls = MagicMock()
    mapper = MagicMock()
    provider = _make_provider(build_return=(orm_cls, mapper))
    provider.register(_User)

    assert _User in provider._built
    stored = provider._built[_User]
    assert stored == (orm_cls, mapper)


# ── get_repository() ──────────────────────────────────────────────────────────


def test_get_repository_returns_async_sqlalchemy_repository() -> None:
    """get_repository() returns an AsyncSQLAlchemyRepository for a registered class."""
    provider = _make_provider()
    provider.register(_User)

    repo = provider.get_repository(_User)

    assert isinstance(repo, AsyncSQLAlchemyRepository)


def test_get_repository_raises_key_error_for_unregistered_class() -> None:
    """get_repository() raises KeyError when the entity was never registered."""
    provider = _make_provider()

    with pytest.raises(KeyError, match="_User"):
        provider.get_repository(_User)


def test_get_repository_error_message_names_fix() -> None:
    """KeyError message mentions register() so the caller knows how to fix it."""
    provider = _make_provider()

    with pytest.raises(KeyError, match="register"):
        provider.get_repository(_User)


# ── make_uow() ────────────────────────────────────────────────────────────────


def test_make_uow_returns_sqlalchemy_unit_of_work() -> None:
    """make_uow() returns a SQLAlchemyUnitOfWork instance."""
    provider = _make_provider()
    provider.register(_User)

    uow = provider.make_uow()

    assert isinstance(uow, SQLAlchemyUnitOfWork)


def test_make_uow_repo_attr_names_lowercase_plus_s() -> None:
    """
    Repository attribute names are derived as lower(ClassName) + 's'.

    User → 'users', Post → 'posts'
    """
    provider = _make_provider()
    provider.register(_User, _Post)

    uow = provider.make_uow()

    assert "users" in uow._repo_factories
    assert "posts" in uow._repo_factories


def test_make_uow_passes_session_factory() -> None:
    """The session_factory callable is forwarded to the UoW."""
    mock_session_factory = MagicMock()

    with patch("fastrest_sa.provider.SAModelFactory"):
        provider = SQLAlchemyRepositoryProvider(
            base=MagicMock(spec=DeclarativeBase),
            session_factory=mock_session_factory,
        )

    provider._built[_User] = (MagicMock(), MagicMock())  # pre-populate
    uow = provider.make_uow()

    # The UoW should hold our session factory
    assert uow._session_factory is mock_session_factory


def test_make_uow_repo_factories_produce_async_sa_repositories() -> None:
    """
    Each repo factory in the UoW creates an AsyncSQLAlchemyRepository
    when called with a session.
    """
    provider = _make_provider()
    provider.register(_User)

    uow = provider.make_uow()
    mock_session = MagicMock()

    # Invoke the factory as the UoW._begin() would
    repo = uow._repo_factories["users"](mock_session)

    assert isinstance(repo, AsyncSQLAlchemyRepository)


# ── _get_built() ──────────────────────────────────────────────────────────────


def test_get_built_raises_key_error_for_unknown_entity() -> None:
    """_get_built() raises KeyError for an entity that was never registered."""
    provider = _make_provider()

    with pytest.raises(KeyError, match="_User"):
        provider._get_built(_User)


def test_get_built_returns_stored_tuple() -> None:
    """_get_built() returns the exact (orm_cls, mapper) tuple from the cache."""
    orm_cls = MagicMock()
    mapper = MagicMock()
    provider = _make_provider(build_return=(orm_cls, mapper))
    provider.register(_User)

    result = provider._get_built(_User)

    assert result == (orm_cls, mapper)
