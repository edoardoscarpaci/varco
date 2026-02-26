# fastrest

This repository contains `fastrest`, a small framework and utilities to build expressive query APIs that translate AST-style query expressions into SQLAlchemy filters. Recent work improved the type coercion subsystem and made field-level coercion more flexible and robust.

**Quick summary of recent changes**
- **Robust list coercion**: `coerce_list` accepts JSON arrays, bracketed lists, comma-separated strings, iterables, and single values.
- **Column-level coercers**: the registry now prefers a coercer defined on a SQLAlchemy `Column` via `Column.info['coercer']`.
- **Element coercion for IN queries**: `ASTTypeCoercion` normalizes `IN` and list-like inputs before applying element coercers.
- **Programmatic defaults**: added `register_default_coercer()` to override or add default coercers by Python type.

See the coercion implementation and registry logic in [fastrest/query/visitor/type_coercion.py](fastrest/query/visitor/type_coercion.py).

**Why these changes?**
- Make the public API for filtering tolerant to common client formats (JSON arrays, comma lists, single values).
- Keep coercion rules colocated with the model when desirable (via `Column.info`) so model authors can specify field-level behavior.
- Preserve an easy programmatic override path for global defaults.

**Files of interest**
- `fastrest/query/visitor/type_coercion.py`: type coercion helpers, `TypeCoercionRegistry`, and `ASTTypeCoercion`.
- `fastrest/query/visitor/sqlalchemy.py`: visitor that converts the AST into SQLAlchemy expressions (unchanged in recent patching; name suggestions were discussed).

**Usage**

1) Configure coercers on your SQLAlchemy model columns (optional, colocated approach):

```python
from sqlalchemy import Column, String, DateTime
from fastrest.query.visitor.type_coercion import coerce_datetime

class MyModel(Base):
		__tablename__ = 'my_table'
		id = Column(Integer, primary_key=True)
		created_at = Column(DateTime, info={"coercer": coerce_datetime})
		tags = Column(String, info={"coercer": lambda s: s.split(',')})
```

2) Use `TypeCoercionRegistry.get_default_from_sqlalchemy_model()` to build a registry for a model:

```python
from fastrest.query.visitor.type_coercion import TypeCoercionRegistry, ASTTypeCoercion

registry = TypeCoercionRegistry.get_default_from_sqlalchemy_model(MyModel)
coercer = ASTTypeCoercion(registry)

# Given an AST (e.g. a ComparisonNode for an IN query), apply coercion:
# coerced_ast = coercer.visit(parsed_ast)
```

3) Register or override global defaults programmatically:

```python
from datetime import datetime
from fastrest.query.visitor.type_coercion import register_default_coercer, coerce_datetime

register_default_coercer(datetime, coerce_datetime)
```

4) How `coerce_list` behaves (examples):

- Input: `'["a","b"]'` → JSON parse → `['a', 'b']`
- Input: `'[a,b]'` → bracket-stripped and split → `['a', 'b']`
- Input: `'a,b'` → split → `['a', 'b']`
- Input: `'single'` → `['single']`
- Input: `['x','y']` → returned unchanged → `['x','y']`

5) `ASTTypeCoercion` element coercion for `IN`:

- When visiting a `ComparisonNode` whose operation is `Operation.IN`, `ASTTypeCoercion` will:
	- normalize string/iterable inputs into a Python `list` (using `coerce_list` for strings),
	- then apply the per-field coercer to each element.

**Design notes & recommendations**
- Prefer `Column.info['coercer']` for model-specific coercion behavior. This keeps the model expressive and self-describing.
- Use `register_default_coercer()` for global type-based rules (e.g., `datetime`, `int`).
- Keep coercers pure and idempotent: they should accept their input and either return a coerced value or raise `fastrest.exception.query.CoercionError` on failure.
- Consider adding unit tests that assert behavior for typical client payloads (JSON arrays, bracketed strings, CSV strings, and single values).

**Examples and snippets**

- Build a registry that prioritizes column-defined coercers, then field-specific overrides, then type defaults:

```python
from fastrest.query.visitor.type_coercion import TypeCoercionRegistry

registry = TypeCoercionRegistry.get_default_from_sqlalchemy_model(MyModel, field_coercions={
		'special_field': lambda x: custom_coerce(x)
})
```

- Apply AST coercion (walk the AST and return a new coerced AST):

```python
coercer = ASTTypeCoercion(registry)
coerced_ast = coercer.visit(parsed_ast)
```

**Testing locally**

If you add tests, run them with `pytest` (if available in your environment):

```bash
python -m pytest
```

**Next steps you might want**
- Add unit tests for `coerce_list` covering the various input cases.
- Document examples in a `docs/` folder or add an example script under `examples/` that demonstrates parsing a query, coercing types, and converting to SQLAlchemy filters.
- Consider adding a decorator API `@coercer_for(field_name)` to register field coercers programmatically in model modules.

---

If you want, I can:
- add a simple `examples/` script demonstrating end-to-end parsing → coercion → SQLAlchemy filter,
- or create unit tests for `type_coercion.py` now. Which would you prefer?