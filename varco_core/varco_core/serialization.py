"""
varco_core.serialization
========================
General-purpose serialization protocol for varco.

Provides a pluggable ``Serializer[T]`` protocol used by both the event bus
backends (``varco_kafka``, ``varco_redis``) and the cache backends
(``varco_redis.cache``, ``varco_memcached``).

Separating serialization from the bus/cache implementations means:

- Backends are not tied to JSON — a user can plug in msgpack, protobuf, etc.
- ``JsonSerializer`` is stateless and safe to share across many components.
- ``NoOpSerializer`` enables raw-bytes usage without any encoding overhead.

DESIGN: Protocol over ABC for the serializer contract
    ✅ Structural typing — any object with the right method signatures satisfies
       the protocol without inheriting from a base class.  Third-party serializers
       (orjson, msgpack, protobuf) can be wrapped without subclassing.
    ✅ ``runtime_checkable`` enables ``isinstance`` guards in bus/cache code.
    ❌ Runtime ``isinstance`` only checks method *existence*, not signatures.
       Full safety requires a static type checker (mypy / pyright).
    ❌ No default implementations in the Protocol itself — callers that want
       behaviour should depend on ``JsonSerializer``, not ``Serializer`` directly.

Thread safety:  ✅ All built-in implementations are stateless — safe to share.
Async safety:   ✅ No I/O — serialization is synchronous and CPU-bound.

📚 Docs
- 🐍 https://docs.python.org/3/library/typing.html#typing.Protocol
  Protocol — structural subtyping (static duck typing)
- 🔍 https://docs.pydantic.dev/latest/concepts/serialization/
  Pydantic serialization — ``TypeAdapter``, ``model_dump``, ``dump_json``
"""

from __future__ import annotations

import importlib
import json
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import TypeAdapter
from pydantic import PydanticSchemaGenerationError

# T is the Python type this serializer handles.
# Covariant would be ideal but Protocols with covariant type vars and
# methods that take T as input are not valid — invariant is correct here.
T = TypeVar("T")


# ── Serializer Protocol ────────────────────────────────────────────────────────


@runtime_checkable
class Serializer(Protocol[T]):
    """
    Structural protocol for pluggable value serializers.

    Any object that exposes ``serialize`` and ``deserialize`` with compatible
    signatures satisfies this protocol — no inheritance required.

    Type parameter:
        T: The Python type this serializer handles.

    Implementations must be stateless and thread-safe unless explicitly
    documented otherwise.

    DESIGN: ``type_hint`` is optional (``None`` default)
        Allows self-describing serializers (e.g. ``JsonEventSerializer`` that
        embeds the event type name in the JSON envelope) to implement the
        protocol without using the ``type_hint`` parameter.  Non-self-describing
        serializers (e.g. ``JsonSerializer`` for cache values) require
        ``type_hint`` to reconstruct the correct Python type.

    Thread safety:  ✅ Stateless by contract — implementations must not hold
                       mutable shared state.
    Async safety:   ✅ No I/O — serialization is synchronous.

    Example::

        s: Serializer[MyModel] = JsonSerializer()
        raw = s.serialize(my_model)
        back = s.deserialize(raw, type_hint=MyModel)
        assert back == my_model
    """

    def serialize(self, value: T) -> bytes:
        """
        Convert ``value`` to a ``bytes`` representation.

        Args:
            value: The object to serialize.

        Returns:
            Serialized bytes.

        Raises:
            TypeError:  If ``value`` contains a type that cannot be serialized.
            ValueError: If serialization fails for semantic reasons.
        """
        ...

    def deserialize(self, data: bytes, type_hint: type[T] | None = None) -> T:
        """
        Reconstruct a ``T`` from its ``bytes`` representation.

        Args:
            data:      Bytes previously produced by ``serialize()``.
            type_hint: The expected return type.  Required for generic or
                       structured types (e.g. Pydantic models, dataclasses).
                       May be ``None`` for self-describing formats where
                       the type is embedded in the data itself.

        Returns:
            A deserialized instance of type ``T``.

        Raises:
            ValueError:        If ``data`` is malformed or missing required keys.
            ValidationError:   (Pydantic) If ``type_hint`` is a Pydantic model
                               and the data does not match its schema.
        """
        ...


# ── JsonSerializer ────────────────────────────────────────────────────────────


class JsonSerializer:
    """
    Serializer that encodes values to UTF-8 JSON using Pydantic's ``TypeAdapter``.

    Handles any type supported by Pydantic's serialization layer, including
    ``BaseModel`` subclasses, dataclasses, primitives, ``datetime``, ``UUID``,
    ``list``, ``dict``, and arbitrarily nested combinations of the above.

    DESIGN: Pydantic TypeAdapter over ``json.dumps(default=...)``
        ✅ Handles Pydantic models, dataclasses, datetime, UUID automatically —
           no custom encoder function needed.
        ✅ Validates on deserialization — catches schema mismatches early.
        ✅ ``dump_json(mode="json")`` guarantees JSON-serializable output even
           for complex nested types.
        ❌ ``TypeAdapter(type(value))`` is constructed per-call — small overhead
           that is negligible for typical call rates.  Cache the adapter externally
           if calling this in a tight inner loop (>100k calls/second).

    Thread safety:  ✅ Stateless — each call constructs its own TypeAdapter.
    Async safety:   ✅ No I/O.

    Example::

        s = JsonSerializer()
        data = s.serialize(my_pydantic_model)
        back = s.deserialize(data, type_hint=type(my_pydantic_model))
        assert back == my_pydantic_model
    """

    def serialize(self, value: Any) -> bytes:
        """
        Serialize ``value`` to UTF-8 JSON bytes.

        Uses ``pydantic.TypeAdapter(type(value)).dump_json()`` — handles
        Pydantic models, dataclasses, datetimes, UUIDs, and primitives.

        Args:
            value: Any Python value.  Pydantic models, dataclasses, TypedDicts,
                   datetimes, UUIDs, and primitives are handled natively.
                   Plain classes without a Pydantic schema fall back to
                   ``json.dumps`` via ``__dict__``.

        Returns:
            UTF-8 JSON bytes.

        Raises:
            TypeError:  If ``value`` contains an unserializable type and has
                        no ``__dict__`` (e.g. a C extension object).
            ValueError: If Pydantic's serializer encounters a semantic error.

        Edge cases:
            - ``None`` → ``b"null"``
            - ``datetime`` → ISO-8601 string bytes.
            - ``UUID`` → hex string bytes.
            - Empty ``dict`` → ``b"{}"``
            - Plain Python class → serialized via ``__dict__``; deserialized
              back as a plain ``dict`` (type information is not preserved).
        """
        # TypeAdapter(type(value)) builds a per-call adapter — acceptable for
        # typical usage rates.  The adapter knows the full schema of the type
        # so nested models serialize correctly without a custom default hook.
        # DESIGN: fall back to json.dumps for plain classes.
        #   ✅ Handles arbitrary domain objects without requiring them to be
        #      Pydantic models — keeps tests and user code simple.
        #   ❌ Deserialization is lossy: plain classes round-trip as dicts.
        #      Callers that need typed round-trips must pass a ``type_hint``
        #      that is Pydantic-compatible to ``deserialize()``.
        try:
            adapter: TypeAdapter[Any] = TypeAdapter(type(value))
            return adapter.dump_json(value)
        except PydanticSchemaGenerationError:
            # Plain Python class — serialize via __dict__ or repr as last resort.
            return json.dumps(
                value,
                default=lambda o: vars(o) if hasattr(o, "__dict__") else repr(o),
            ).encode()

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
        """
        Deserialize UTF-8 JSON bytes back to a Python value.

        Args:
            data:      UTF-8 JSON bytes produced by ``serialize()``.
            type_hint: The expected return type.  If ``None``, returns a raw
                       Python object (``dict``, ``list``, ``str``, etc.).
                       Provide a Pydantic model class to get a validated instance.

        Returns:
            Deserialized Python value — type matches ``type_hint`` when provided.

        Raises:
            ValueError:      If ``data`` is not valid JSON.
            ValidationError: (Pydantic) If ``type_hint`` is a Pydantic model and
                             the data does not conform to its schema.

        Edge cases:
            - ``type_hint=None`` → falls back to ``json.loads`` → plain dicts/lists.
            - ``b"null"`` with ``type_hint=None`` → returns Python ``None``.
        """
        if type_hint is None:
            # No schema to validate against — return raw Python objects.
            # json.loads is used directly here since TypeAdapter(Any) does the
            # same thing but with unnecessary overhead.
            return json.loads(data)

        # TypeAdapter validates the parsed JSON against the provided schema,
        # coercing types (e.g. str → UUID, str → datetime) as needed.
        adapter: TypeAdapter[Any] = TypeAdapter(type_hint)
        return adapter.validate_json(data)

    def __repr__(self) -> str:
        return "JsonSerializer()"


# ── NoOpSerializer ────────────────────────────────────────────────────────────


class NoOpSerializer:
    """
    Pass-through serializer for values that are already serialized ``bytes``.

    ``serialize`` returns its input unchanged; ``deserialize`` returns the
    raw ``bytes`` unchanged.  Useful when:

    - The value is already in bytes form (pre-serialized payloads).
    - You want to bypass any encoding layer entirely.
    - Benchmarking without serialization overhead.

    DESIGN: identity transform
        ✅ Zero overhead — no encoding or decoding performed.
        ❌ Only safe for ``bytes`` values — ``serialize(non_bytes)`` raises
           ``TypeError`` immediately to surface the mistake.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``serialize(non_bytes)`` raises ``TypeError`` — intentional guard.
          The caller must ensure the value is already bytes.
        - ``deserialize`` always returns the raw bytes regardless of ``type_hint``.
          ``type_hint`` is accepted only for API compatibility and is ignored.
    """

    def serialize(self, value: bytes) -> bytes:
        """
        Return ``value`` unchanged.

        Args:
            value: Must be ``bytes``.

        Returns:
            The same ``bytes`` object.

        Raises:
            TypeError: If ``value`` is not ``bytes``.
        """
        if not isinstance(value, bytes):
            raise TypeError(
                f"NoOpSerializer.serialize() expects bytes, "
                f"got {type(value).__name__!r}. "
                f"Use JsonSerializer for non-bytes values."
            )
        return value

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> bytes:
        """
        Return ``data`` unchanged.

        Args:
            data:      Raw bytes.
            type_hint: Accepted for API compatibility — always ignored.

        Returns:
            The same ``bytes`` object passed in.
        """
        # type_hint is intentionally ignored — this serializer is a pure passthrough.
        _ = type_hint
        return data

    def __repr__(self) -> str:
        return "NoOpSerializer()"


# ── TypedJsonSerializer ───────────────────────────────────────────────────────


class TypedJsonSerializer:
    """
    Self-describing serializer that embeds the fully-qualified class name in the
    serialized envelope so the original type can be recovered at deserialization
    time without passing a ``type_hint``.

    Wire format::

        {
            "__type__": "myapp.models.Order",
            "__data__": { ...Pydantic-serialized fields... }
        }

    This is the general-purpose analogue of ``JsonEventSerializer``, which uses
    the same envelope idea but only for ``Event`` subclasses registered in
    ``Event._registry``.  ``TypedJsonSerializer`` works for **any** Pydantic
    model, dataclass, or plain Python class.

    DESIGN: Typed envelope over a separate type registry
        ✅ Zero setup — no explicit registration step required.  Any importable
           class just works.
        ✅ Human-readable — the JSON envelope is inspectable and debuggable.
        ✅ ``type_hint`` override — callers can still supply an explicit type to
           control deserialization (e.g. for migration or coercion).
        ✅ Backwards-compatible read — payloads without ``__type__`` delegate to
           ``JsonSerializer`` for graceful handling of legacy bytes.
        ❌ Couples bytes to the class's module path.  Renaming or moving a class
           breaks existing payloads — plan a migration strategy before refactoring
           serialized types.
        ❌ Deserialization imports the module named in ``__type__``.  If untrusted
           input controls the bytes, an attacker could forge ``__type__`` to import
           arbitrary modules (deserialization gadget attack).  Use ``allow_modules``
           to restrict which module prefixes are permitted.

    Args:
        allow_modules: Optional set of module-path prefixes that are permitted
            during deserialization.  When ``None`` (default) any module may be
            imported — suitable for closed, internal systems.  Set this to a
            prefix allowlist (e.g. ``{"myapp.", "varco_core."}``) when
            deserializing data that arrives from an external or untrusted source.

    Thread safety:  ✅ Stateless after construction — ``allow_modules`` is
                       read-only.  Safe to share across threads and tasks.
    Async safety:   ✅ No I/O performed — serialization and deserialization are
                       synchronous CPU-bound operations.

    Example::

        s = TypedJsonSerializer()

        # No type_hint needed — the class is embedded in the bytes
        data = s.serialize(my_order)
        back = s.deserialize(data)
        assert isinstance(back, Order)
        assert back == my_order

        # With allowlist (recommended for untrusted input)
        safe = TypedJsonSerializer(allow_modules={"myapp.", "varco_core."})
        back = safe.deserialize(data)

    📚 Docs
    - 🐍 https://docs.python.org/3/library/importlib.html
      importlib — dynamic import used to resolve ``__type__`` back to a class
    - 🐍 https://docs.python.org/3/library/stdtypes.html#definition.__qualname__
      __qualname__ — preserves nested class names (e.g. ``Outer.Inner``)
    - 🔍 https://docs.pydantic.dev/latest/concepts/serialization/
      Pydantic serialization — ``TypeAdapter``, round-trips via ``dump_json``
    """

    def __init__(self, allow_modules: set[str] | None = None) -> None:
        """
        Args:
            allow_modules: Optional allowlist of module-path prefixes.  When set,
                ``deserialize()`` rejects any ``__type__`` whose module does not
                start with one of the listed prefixes.  ``None`` disables the
                check (open trust model).
        """
        # Stored as a frozenset for O(1) iteration safety — the set is never
        # mutated after construction so we can avoid a copy on every call.
        self._allow_modules: frozenset[str] | None = (
            frozenset(allow_modules) if allow_modules is not None else None
        )
        # Reuse a single JsonSerializer instance — it is stateless and safe to
        # share, and avoids repeated module-level imports on every call.
        self._json = JsonSerializer()

    def serialize(self, value: Any) -> bytes:
        """
        Serialize ``value`` to a self-describing JSON envelope.

        The envelope contains two keys:

        - ``"__type__"`` — fully-qualified class name
          (``"{module}.{qualname}"``), e.g. ``"myapp.models.Order"``.
        - ``"__data__"`` — the value serialized by ``JsonSerializer``.

        Args:
            value: Any Python value.  Pydantic models, dataclasses, datetimes,
                UUIDs, and primitives are supported (same as ``JsonSerializer``).

        Returns:
            UTF-8 JSON bytes of the typed envelope.

        Raises:
            TypeError:  If ``value`` contains an unserializable type.
            ValueError: If Pydantic's serializer encounters a semantic error.

        Edge cases:
            - ``None`` → ``__type__`` is ``"builtins.NoneType"``;
              ``__data__`` is ``null``.
            - Inner/nested classes → ``__qualname__`` preserves the nesting
              (e.g. ``"Outer.Inner"``), so the full path is
              ``"module.Outer.Inner"``.  ``_import_class`` handles this by
              walking the attribute chain.
        """
        # Build the fully-qualified name using __qualname__ rather than __name__
        # so that inner/nested classes round-trip correctly.  For example, if
        # class Inner is defined inside class Outer, __qualname__ = "Outer.Inner"
        # whereas __name__ = "Inner" — the latter is not importable.
        qualified = f"{type(value).__module__}.{type(value).__qualname__}"

        # Serialize via JsonSerializer first so we get Pydantic's full treatment
        # (UUID → str, datetime → ISO-8601, nested models, etc.), then parse
        # back to a plain dict so we can embed it as the __data__ value.
        inner_bytes: bytes = self._json.serialize(value)
        inner_obj: Any = json.loads(inner_bytes)

        envelope = {"__type__": qualified, "__data__": inner_obj}
        return json.dumps(envelope).encode()

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
        """
        Deserialize a typed envelope back to the original Python value.

        Resolution order:

        1. ``type_hint`` — used directly if provided (caller override wins).
        2. ``__type__`` key in the envelope — imported via ``importlib``.
        3. No envelope keys — delegates to ``JsonSerializer`` (legacy/raw bytes).

        Args:
            data:      UTF-8 JSON bytes previously produced by ``serialize()``,
                       or a raw (non-envelope) payload for backwards compatibility.
            type_hint: Optional explicit type override.  When supplied, the
                       embedded ``__type__`` is ignored and ``type_hint`` is used
                       for Pydantic validation — useful for schema migration.

        Returns:
            A deserialized instance of the original type, or of ``type_hint``
            when that override is present.

        Raises:
            ValueError:      If ``data`` is not valid JSON, if the ``__type__``
                             module is not in ``allow_modules``, or if the class
                             attribute cannot be found on the resolved module.
            ImportError:     If the module named in ``__type__`` cannot be found.
            ValidationError: (Pydantic) If the data does not match the schema.

        Edge cases:
            - Bytes without ``__type__`` key → delegates to ``JsonSerializer``
              so old non-envelope payloads are handled gracefully.
            - ``type_hint`` overrides ``__type__`` entirely — the allowlist is
              not checked when the caller explicitly names the type.
            - ``b"null"`` → depends on the resolved type; Pydantic may accept
              ``None`` or raise a ``ValidationError``.
        """
        raw: Any = json.loads(data)

        if not isinstance(raw, dict) or "__type__" not in raw:
            # Not an envelope — fall back to JsonSerializer for backwards compat.
            # This handles plain bytes produced by JsonSerializer directly, or
            # any non-dict payload (list, int, etc.).
            return self._json.deserialize(data, type_hint)

        qualified: str = raw["__type__"]

        # type_hint wins over the embedded class — lets callers coerce/migrate
        # types without changing the serialized bytes.
        cls: type = (
            type_hint if type_hint is not None else self._import_class(qualified)
        )

        # Re-encode __data__ as bytes so JsonSerializer's TypeAdapter path
        # handles coercion (str → UUID, str → datetime, nested models, etc.).
        inner_bytes: bytes = json.dumps(raw["__data__"]).encode()

        # DESIGN: PydanticSchemaGenerationError fallback for plain classes
        #   Plain Python classes without Pydantic schema (no BaseModel, no
        #   dataclass, no __get_pydantic_core_schema__) raise
        #   PydanticSchemaGenerationError when TypeAdapter is constructed.
        #   In this case we fall back to returning the raw deserialized dict,
        #   which matches the documented behaviour of JsonSerializer for the
        #   same type.  Type information is embedded in __type__ but the value
        #   is lost on the deserialization side.
        #   ✅ Graceful — callers that only need the data still get it.
        #   ❌ Lossy — the Python type is not reconstructed.  Users should
        #      migrate plain classes to Pydantic models or dataclasses for
        #      full round-trip fidelity.
        try:
            return self._json.deserialize(inner_bytes, cls)
        except PydanticSchemaGenerationError:
            # cls has no Pydantic schema — return the raw dict so callers
            # at least get the data even though the type is lost.
            return self._json.deserialize(inner_bytes)

    def _import_class(self, qualified: str) -> type:
        """
        Resolve a fully-qualified class name to a Python type object.

        Handles nested classes by walking the attribute chain after the initial
        import.  For example, ``"myapp.models.Outer.Inner"`` will:

        1. Try ``importlib.import_module("myapp.models.Outer.Inner")`` — fails.
        2. Fall back to ``importlib.import_module("myapp.models")``
           + ``getattr(module, "Outer")`` + ``getattr(outer, "Inner")``.

        Args:
            qualified: Fully-qualified name as stored in ``__type__``.

        Returns:
            The resolved Python type.

        Raises:
            ValueError:  If ``allow_modules`` is set and the module prefix is
                         not in the allowlist.
            ImportError: If the module cannot be imported.
            ValueError:  If the attribute chain cannot be resolved on the module.

        Edge cases:
            - ``"builtins.int"`` → resolves to ``int`` — primitives work correctly.
            - Nested class ``"myapp.Outer.Inner"`` → walks attributes correctly.
        """
        # Split on the last '.' to extract module path and attribute name.
        # For plain classes this is unambiguous.  For nested classes we may need
        # to walk further — handled by the loop below.
        module_path, _, attr = qualified.rpartition(".")

        if not module_path:
            # Bare name with no module — unexpected but handle gracefully.
            raise ValueError(
                f"Cannot resolve class from {qualified!r}: no module path found. "
                f"Expected a fully-qualified name like 'myapp.models.Order'."
            )

        # Allowlist check — performed before any import to avoid side-effects.
        if self._allow_modules is not None:
            permitted = any(
                module_path.startswith(prefix) for prefix in self._allow_modules
            )
            if not permitted:
                raise ValueError(
                    f"Module {module_path!r} is not in the allow_modules allowlist "
                    f"({sorted(self._allow_modules)!r}). "
                    f"Add the module prefix to allow_modules if this is intentional."
                )

        # Try importing module_path directly.  For top-level classes this
        # succeeds immediately.  For nested classes (e.g. "myapp.Outer.Inner")
        # the last segment "Outer" is not a module — we catch ImportError and
        # retry with progressively shorter module paths.
        parts = qualified.split(".")
        # Walk from the longest possible module path down to a single segment.
        module = None
        attr_chain: list[str] = []
        for i in range(len(parts) - 1, 0, -1):
            candidate_module = ".".join(parts[:i])
            candidate_attrs = parts[i:]
            try:
                module = importlib.import_module(candidate_module)
                attr_chain = candidate_attrs
                break
            except ImportError:
                continue

        if module is None:
            raise ImportError(
                f"Could not import any module from {qualified!r}. "
                f"Ensure the module is installed and importable."
            )

        # Walk the attribute chain to resolve nested classes.
        obj: Any = module
        for segment in attr_chain:
            try:
                obj = getattr(obj, segment)
            except AttributeError:
                # Some built-in types (NoneType, FunctionType, etc.) are NOT
                # attributes of the `builtins` module but ARE in the `types`
                # module (available since Python 3.10).  Try there before giving
                # up so that round-tripping None, functions, etc. works.
                import types as _types_module  # local import — rare code path

                fallback = getattr(_types_module, segment, None)
                if fallback is not None:
                    obj = fallback
                else:
                    raise ValueError(
                        f"Attribute {segment!r} not found while resolving {qualified!r}. "
                        f"The class may have been renamed or moved. "
                        f"Current object: {obj!r}."
                    ) from None

        if not isinstance(obj, type):
            raise ValueError(
                f"Resolved {qualified!r} to {obj!r}, which is not a type. "
                f"Expected a class, got {type(obj).__name__!r}."
            )

        return obj

    def __repr__(self) -> str:
        allow = (
            f"allow_modules={sorted(self._allow_modules)!r}"
            if self._allow_modules is not None
            else "allow_modules=None"
        )
        return f"TypedJsonSerializer({allow})"


__all__ = [
    "Serializer",
    "JsonSerializer",
    "NoOpSerializer",
    "TypedJsonSerializer",
]
