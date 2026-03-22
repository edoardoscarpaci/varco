"""
varco_beanie
================
Beanie (pymongo / MongoDB) async backend for varco.

Quick start::

    from pymongo import AsyncMongoClient
    from varco_beanie import BeanieRepositoryProvider, BeanieDocRegistry
    from varco_core.query.builder import QueryBuilder
    from varco_core.query.params import QueryParams
    from varco_core.query.type import SortField, SortOrder

    provider = BeanieRepositoryProvider(mongo_client=AsyncMongoClient(...), db_name="mydb")
    provider.register(User, Post)
    await provider.init()

    async with provider.make_uow() as uow:
        recent_posts = await uow.posts.find_by_query(
            QueryParams(
                node=QueryBuilder().eq("published", True).build(),
                sort=[SortField("created_at", SortOrder.DESC)],
                limit=10,
            )
        )

providify DI integration::

    from pymongo import AsyncMongoClient
    from varco_beanie import BeanieModule, BeanieSettings, bind_repositories
    from providify import DIContainer, Provider

    container = DIContainer()

    @Provider(singleton=True)
    def settings() -> BeanieSettings:
        return BeanieSettings(mongo_client=AsyncMongoClient(...), db_name="mydb", entity_classes=(User,))

    container.provide(settings)
    container.install(BeanieModule)
    bind_repositories(container, User)

    repo = await container.aget(AsyncRepository[User])
"""

from varco_beanie.bootstrap import BeanieConfig, BeanieFastrestApp
from varco_beanie.di import BeanieModule, BeanieSettings, bind_repositories
from varco_beanie.factory import BeanieDocRegistry, BeanieModelFactory
from varco_beanie.provider import BeanieRepositoryProvider
from varco_beanie.repository import AsyncBeanieRepository
from varco_beanie.uow import BeanieUnitOfWork

__all__ = [
    # Core backend classes
    "BeanieModelFactory",
    "BeanieDocRegistry",
    "AsyncBeanieRepository",
    "BeanieUnitOfWork",
    "BeanieRepositoryProvider",
    # DI integration
    "BeanieSettings",
    "BeanieModule",
    "bind_repositories",
    # Bootstrap
    "BeanieConfig",
    "BeanieFastrestApp",
]
