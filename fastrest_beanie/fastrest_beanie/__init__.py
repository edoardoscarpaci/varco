"""
fastrest_beanie
================
Beanie (Motor / MongoDB) async backend for fastrest.

Quick start::

    from fastrest_beanie import BeanieRepositoryProvider, BeanieDocRegistry
    from fastrest_core.query.builder import QueryBuilder
    from fastrest_core.query.params import QueryParams
    from fastrest_core.query.type import SortField, SortOrder

    provider = BeanieRepositoryProvider(motor_client=client, db_name="mydb")
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
"""

from fastrest_beanie.factory import BeanieDocRegistry, BeanieModelFactory
from fastrest_beanie.provider import BeanieRepositoryProvider
from fastrest_beanie.repository import AsyncBeanieRepository
from fastrest_beanie.uow import BeanieUnitOfWork

__all__ = [
    "BeanieModelFactory",
    "BeanieDocRegistry",
    "AsyncBeanieRepository",
    "BeanieUnitOfWork",
    "BeanieRepositoryProvider",
]
