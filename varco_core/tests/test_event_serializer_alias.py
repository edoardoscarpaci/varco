"""
Regression tests for the event-serializer DI seam.

History guarded here
--------------------
``varco_core.event.serializer`` used to export a *quoted* type alias::

    if TYPE_CHECKING:
        from varco_core.event.base import Event

    EventSerializer: TypeAlias = "Serializer[Event]"

Because the right-hand side was a string literal, the module-level name was
bound at runtime to the ``str`` ``"Serializer[Event]"`` rather than to a type.
Every bus backend annotated its constructor with
``Annotated[EventSerializer | None, InjectMeta(optional=True)]``, which then
evaluated ``str | None`` → ``TypeError: unsupported operand type(s) for |``.
Under providify < 1.1.0 that was swallowed into an empty hints dict, silently
dropping DI for the parameter; under >= 1.1.0 it aborts container validation.

The alias is gone. Bus constructors now annotate the **interface**
``Serializer[Event]`` directly, and ``JsonEventSerializer`` explicitly
subclasses it and carries ``@Fallback`` (Plan 045) so it is the in-process
default binding — present out of the box, and any application-supplied
serializer wins regardless of registration order.

These tests pin that contract end-to-end: the interface resolves, the default
is ``JsonEventSerializer``, and an app override wins.
"""

from __future__ import annotations

from typing import Annotated, Any, get_type_hints

from providify import DIContainer, Provider
from varco_core.event.base import Event
from varco_core.event.serializer import JsonEventSerializer
from varco_core.serialization import Serializer


class _CustomSerializer(Serializer[Event]):
    """Stand-in for an application-supplied serializer."""

    def serialize(self, value: Any) -> bytes:
        return b"{}"

    def deserialize(self, data: bytes, type_hint: Any = None) -> Any:
        return None


@Provider(singleton=True)
def _custom_serializer() -> Serializer[Event]:
    """App-supplied override — module scope so its lazy annotation resolves."""
    return _CustomSerializer()


class TestSerializerAnnotationsAreResolvable:
    def test_interface_is_a_runtime_type_not_a_string(self) -> None:
        """The original bug was literally ``the annotation target is a str``."""
        assert not isinstance(Serializer[Event], str)

    def test_interface_supports_union_with_none(self) -> None:
        """``Serializer[Event] | None`` is what every bus constructor writes."""
        union = Serializer[Event] | None

        assert type(None) in union.__args__

    def test_lazy_annotation_using_the_interface_resolves(self) -> None:
        """
        Mirrors exactly what providify's binding validation does —
        ``get_type_hints`` over a PEP-563 lazily-annotated function.
        """

        def target(serializer: Serializer[Event] | None = None) -> None: ...

        target.__annotations__["serializer"] = "Serializer[Event] | None"

        hints = get_type_hints(
            target,
            globalns={"Serializer": Serializer, "Event": Event},
            include_extras=True,
        )

        assert "serializer" in hints

    def test_annotated_injectmeta_form_resolves(self) -> None:
        """The precise shape used by the Kafka/Redis/NATS bus constructors."""

        def target(
            serializer: Annotated[Serializer[Event] | None, "marker"] = None,
        ) -> None: ...

        target.__annotations__["serializer"] = 'Annotated[Serializer[Event] | None, "marker"]'

        hints = get_type_hints(
            target,
            globalns={
                "Annotated": Annotated,
                "Serializer": Serializer,
                "Event": Event,
            },
            include_extras=True,
        )

        assert "serializer" in hints


class TestJsonEventSerializerIsTheDefaultBinding:
    def test_subclasses_the_interface(self) -> None:
        """
        Explicit subclassing (not just structural Protocol satisfaction) is what
        lets providify register the class under ``Serializer[Event]``.
        """
        assert issubclass(JsonEventSerializer, Serializer)

    def test_registered_as_a_fallback_at_priority_zero(self) -> None:
        """
        `@Fallback` is the whole point: the default must lose to any
        application-supplied binding, regardless of registration order or
        priority — it carries no explicit priority (providify's default, 0).
        """
        container = DIContainer()
        container.scan("varco_core", recursive=True)

        matching = [
            b
            for b in container._bindings
            if getattr(b, "implementation", None) is JsonEventSerializer
        ]

        assert {b.priority for b in matching} == {0}
        assert all(b.fallback is True for b in matching)

    def test_interface_resolves_to_the_default(self) -> None:
        container = DIContainer()
        container.scan("varco_core", recursive=True)

        assert isinstance(container.get(Serializer[Event]), JsonEventSerializer)

    def test_default_is_a_singleton(self) -> None:
        container = DIContainer()
        container.scan("varco_core", recursive=True)

        assert container.get(Serializer[Event]) is container.get(Serializer[Event])

    def test_application_serializer_wins_over_the_default(self) -> None:
        """
        Regression: before the alias fix the parameter was never injected at
        all, so an app-supplied serializer was silently ignored and every bus
        used ``JsonEventSerializer``.
        """
        container = DIContainer()
        container.provide(_custom_serializer)
        container.scan("varco_core", recursive=True)

        assert isinstance(container.get(Serializer[Event]), _CustomSerializer)
