"""
example
=======
End-to-end reference implementation of the varco stack.

Demonstrates a complete ``Post`` CRUD service wired through all layers:

    FastAPI router (VarcoCRUDRouter)
        → PostService (AsyncService + CacheServiceMixin)
            → AsyncRepository[Post]       (via SQLAlchemy)
            → CacheBackend                (via Redis)
            → AbstractEventProducer       (via BusEventProducer)
        → PostEventConsumer (EventConsumer + VarcoLifespan)

DI wiring uses Providify throughout.  See ``example.app`` for the
complete bootstrap sequence and ``example.tests`` for unit tests
that run without any external infrastructure.
"""
