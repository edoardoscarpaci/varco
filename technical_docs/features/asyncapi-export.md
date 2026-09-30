# AsyncAPI export — `varco_core.asyncapi` + `varco export-asyncapi`

Plan 030 / Phases 1–2 (BACKLOG 3.1, row **N3**). Prior design:
`plans/022-api-freeze-and-standards-alignment.md` §D-AA1–§D-AA4; name reserved by
`design/api-freeze-and-standards/reserved-seams.md` RS-3. Evidence:
`design/research/004-flags-asyncapi-and-sbom-tooling.md` §2.

Closes: "the only description of which events this service consumes, on which channels, with which
payloads, is the source code."

## What it produces

An **AsyncAPI 3.1.0** document, as a plain `dict` (JSON on the wire):

| varco | AsyncAPI |
|---|---|
| a `@listen` channel string | a **channel**, its `address` |
| the decorated handler | an **operation**, `action: receive` |
| the `Event` subclass | a **message**, `payload` from `model_json_schema()` |

```python
from varco_core.asyncapi import generate_asyncapi

doc = generate_asyncapi(
    container,                    # or a list of live EventConsumer instances
    title="Orders", version="1.0.0",
    protocol="kafka", group_id="orders-workers",
)
```

## Generation is runtime, and that is the whole design

`@listen`'s channel may be a `Callable[[Any], str]` — `_ListenEntry.channel` in
`varco_core/event/consumer.py` — resolved at `register_to()` time against a bound `self`. A static
import walk reports the lambda, or reports one channel for two differently configured instances of
the same class. So the generator takes **live objects**: consumer instances, or a `DIContainer` to
resolve them from.

Two consequences, both correct and both asserted by unit tests:

- **A consumer that was never `register_to()`-ed is absent from the document.** It describes a
  subscription that does not exist. `_is_registered()` reads the public
  `EventConsumer.registered_bus_count` property (Plan 051 / DI-5 — was a private
  `_registered_buses` read before this plan).
- **Two instances of one class produce two channels.** That is the case a static scan gets silently
  wrong, and the reason for the whole approach.

Resolving from a container handles both providify shapes: a binding registered as an
already-constructed *instance* (`container.provide(consumer, returns=MyConsumer)`) is read straight
off the binding — providify would otherwise try to *call* the instance as a provider — and a normal
class binding is resolved. A binding that cannot be resolved is skipped, not fatal: a partially
describable app is more useful than a traceback.

## Binding coverage — Kafka only, and the document says so

| Protocol | Emitted | Why |
|---|---|---|
| **Kafka** | channel `topic`, operation `groupId` (both with `bindingVersion: 0.5.0`) | The binding spec defines real fields |
| **NATS** | operation `queue` — **only when a queue group is configured** | The binding has exactly one field; a stanza carrying only `bindingVersion` is noise |
| **Redis** | nothing, ever | The Redis binding has zero properties at all four levels; `address` already carries everything Redis has |

The same explanation is written into the generated document's own `info.description`, so a reader
wondering why their Redis channels have no bindings finds the answer in the artifact rather than in
a plan file.

## No `servers` block by default

A broker URL is deployment configuration, not source truth, and baking a dev URL into a committed
snapshot is exactly the documentation rot the `--check` gate exists to prevent. Pass
`--server prod=kafka://broker.example.com:9092` (repeatable) to emit one explicitly.

## The CLI verb

```
varco export-asyncapi --title "Orders" --version 1.0.0
                      [--path DIR]...
                      (--consumer module:Class | --source module:callable)...
                      [--protocol kafka|nats|redis] [--group-id G] [--queue-group Q]
                      [--server NAME=protocol://host]...
                      [--output FILE | --check]
```

- `--consumer module:Class` constructs the class and registers it to a throwaway
  `InMemoryEventBus`. Construction is tried as `Class()` and then `Class(bus)` — the common shape
  for a consumer holding an injected bus.
- `--source module:callable` is the honest seam for anything more complex: the callable returns
  already-wired consumers, or the app's own `DIContainer`.
- `--path DIR` prepends a directory to `sys.path` — needed for an app that is not an installed
  package (the example app is exactly this case).

Registered in the **`varco.commands`** entry-point group (`varco_core/pyproject.toml`), the same
group `varco_sa`, `varco_beanie` and `varco_fastapi` already use — not hard-wired into
`cli/main.py`'s built-in list, so the dispatch mechanism has one shape.

⚠️ **JSON only, deliberately.** YAML would need `pyyaml`, which is present in this workspace's dev
environment (via the docs toolchain) but is **not** a `varco_core` runtime dependency — and
`varco_core` takes no new runtime dependency for an output format. Plan 030's Risks table names
JSON-only as the accepted v1; every AsyncAPI tool reads JSON natively.

## The snapshot gate

`make lint` (no-`PKG` path only, beside `api-check` and `import-budget`) runs `make asyncapi-check`,
which regenerates the document from the example app's live consumers and diffs it against
`design/api-freeze-and-standards/measurements/asyncapi-example.json`. Drift exits non-zero.

Regenerate with `make asyncapi`, and commit the result in the same change.

`make lint PKG=<one package>` deliberately skips it, exactly as it skips `api-check` — the §D-C5
rule that keeps a single-package lint narrow and fast.

**No new CI job.** The gate rides in `test.yml`'s existing `lint` job through `make lint`.

## Validating against the spec (local, optional)

The snapshot gate proves the document has not drifted from *our* snapshot. It proves nothing about
*spec conformance*. That was established once, by hand:

```bash
npx -y @asyncapi/cli@latest validate \
    design/api-freeze-and-standards/measurements/asyncapi-example.json
```

Result recorded in `design/api-freeze-and-standards/measurements/asyncapi-validate.txt`:
`@asyncapi/cli/6.0.2`, **valid, no governance issues** — which also settles research 002's
Evidence-gap 1 (AsyncAPI has no blessed `schemaFormat` for JSON Schema Draft 2020-12, and the
validator accepts Pydantic's payload with none declared).

⛔ **This never goes into CI.** `@asyncapi/cli` needs Node 24+, no Python AsyncAPI validator exists,
and §D-AA4 judges a Node toolchain in CI a poor trade for the assurance. Re-run it by hand after a
*structural* change to the generator (a new binding, a new document section); a wiring-only change
is already covered by `make asyncapi-check`.

## Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Documenting consumers that were never wired | The document is empty, or missing handlers | `register_to()` first — or use `--source` and let the app do its own wiring |
| Expecting a static/offline export | — | Not supported, on purpose: a callable channel is unresolvable without a bound instance |
| Regenerating the snapshot blindly when `make lint` fails | The gate stops meaning anything | The snapshot's subject is ONE example app; if it moved and you did not touch that app's wiring, find out why before regenerating |
| Passing `--server` in the committed snapshot command | A dev broker URL lands in a committed artifact | Leave `servers` absent; supply it in the consumer's own tooling |
| Adding `pyyaml` "just for `--format yaml`" | A new `varco_core` runtime dependency | JSON only. YAML output is a follow-up, and would be an optional extra at most |
| Adding a Node step to CI to validate the document | A large operational cost for a small assurance gain | §D-AA4 forbids it; one recorded manual run is the decided posture |

## See also

- `design/api-freeze-and-standards/reserved-seams.md` RS-3 — the reserved module and verb names.
- `plans/022-api-freeze-and-standards-alignment.md` §D-AA1–§D-AA4 — the original decisions.
- `design/research/004-flags-asyncapi-and-sbom-tooling.md` §2 — tooling evidence, and why
  `datamodel-code-generator` was a mis-scoped risk.
- [AsyncAPI 3.1.0 specification](https://www.asyncapi.com/docs/reference/specification/v3.1.0).
