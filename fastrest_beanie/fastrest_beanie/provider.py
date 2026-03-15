"""
fastrest_beanie.provider
=========================
Concrete ``RepositoryProvider`` for Beanie (Motor / MongoDB).
"""

from __future__ import annotations

from typing import Any, TypeVar

from fastrest_core.model import DomainModel
from fastrest_core.providers import RepositoryProvider
from fastrest_core.repository import AsyncRepository

D = TypeVar("D", bound=DomainModel)


class BeanieRepositoryProvider(RepositoryProvider):
    """
    ``RepositoryProvider`` backed by Beanie (Motor / MongoDB).

    Usage::

        from motor.motor_asyncio import AsyncIOMotorClient
        from fastrest_beanie.provider import BeanieRepositoryProvider

        client   = AsyncIOMotorClient("mongodb://localhost:27017")
        provider = BeanieRepositoryProvider(motor_client=client, db_name="myapp")
        provider.register(User, Post)   # ← or autodiscover("myapp.models")

        await provider.init()           # calls init_beanie() at startup

        async with provider.make_uow() as uow:
            user = await uow.users.save(User(name="Edo", email="..."))

    Args:
        motor_client:  Connected ``AsyncIOMotorClient``.
        db_name:       MongoDB database name.
        transactional: Use MongoDB transactions (replica set required).

    Edge cases:
        - ``await provider.init()`` must be called once at startup, after all
          ``register()`` / ``autodiscover()`` calls.
        - Registering new domain classes after ``init()`` requires calling
          ``init()`` again — Beanie must know all Document classes upfront.
    """

    def __init__(
        self,
        motor_client: Any,
        db_name: str,
        *,
        transactional: bool = False,
    ) -> None:
        from fastrest_beanie.factory import BeanieModelFactory

        self._client = motor_client
        self._db_name = db_name
        self._transactional = transactional
        self._factory = BeanieModelFactory()
        self._built: dict[type, tuple[type, Any]] = {}

    def register(self, *domain_classes: type[DomainModel]) -> None:
        for cls in domain_classes:
            if cls not in self._built:
                self._built[cls] = self._factory.build(cls)

    async def init(self) -> None:
        """
        Initialise Beanie with all registered Document classes.

        Must be called once at startup after all ``register()`` /
        ``autodiscover()`` calls.  Idempotent — safe to call multiple times.
        """
        from beanie import init_beanie
        from fastrest_beanie.factory import BeanieDocRegistry

        db = self._client[self._db_name]
        await init_beanie(
            database=db,
            document_models=BeanieDocRegistry.all_documents(),
        )

    def get_repository(self, entity_cls: type[D]) -> AsyncRepository[D, Any]:
        from fastrest_beanie.repository import AsyncBeanieRepository

        _, mapper = self._get_built(entity_cls)
        return AsyncBeanieRepository(mapper=mapper)

    def make_uow(self) -> Any:
        """
        Return a ``BeanieUnitOfWork`` with all registered repos pre-wired.
        """
        from fastrest_beanie.repository import AsyncBeanieRepository
        from fastrest_beanie.uow import BeanieUnitOfWork

        repo_factories = {
            _repo_attr(cls): (lambda _s, m=mapper: AsyncBeanieRepository(mapper=m))
            for cls, (_, mapper) in self._built.items()
        }
        return BeanieUnitOfWork(
            motor_client=self._client,
            repo_factories=repo_factories,
            transactional=self._transactional,
        )

    def _get_built(self, entity_cls: type) -> tuple[type, Any]:
        try:
            return self._built[entity_cls]
        except KeyError:
            raise KeyError(
                f"{entity_cls.__name__!r} is not registered. "
                "Call provider.register(EntityClass) "
                "or provider.autodiscover('myapp.models') first."
            ) from None


def _repo_attr(cls: type) -> str:
    """``User`` → ``'users'``, ``UserRole`` → ``'userroles'``."""
    return cls.__name__.lower() + "s"
