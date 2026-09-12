# Error taxonomy — `message_key`, `params`, and the additive-hybrid envelope

Plan 011 (I1). Closes: "every varco error carries a stable numeric `code`,
but nothing a client can key a translation table off without shipping its
own copy of every English string, and no structured data for
interpolation."

**The D-4 kill switch, up front** — this is the plan's one deliberate wire
delta and it belongs in the first screenful:

```bash
VARCO_ERROR_INCLUDE_MESSAGE_KEY=false   # omit message_key from every body
VARCO_ERROR_INCLUDE_PARAMS=false        # omit params from every body
```

Both `True` by default. Setting both to `false` restores the exact
pre-Plan-011 JSON body for every exception, built-in or not, with no other
code change. See "The one wire delta" below for why the default is *on*
despite this repo's usual off-by-default posture.

A third, related but separate knob — `VARCO_ERROR_INCLUDE_DETAIL` (default `true`,
Plan 035 / S3, §D-S3b) — gates the `detail` member (`str(exc)`) rather than `message_key`/
`params`. See "S3 — the unmapped-exception fallback no longer echoes `str(exc)`" below for the
full picture: this is a **warn-only**, 4.0-flip-candidate knob, not the S3 security fix itself
(that fix is unconditional and has no toggle).

## The envelope, by example

A built-in `ServiceNotFoundError`, default settings:

```json
{
  "code": "FASTREST_001",
  "http_status": 404,
  "message": "The requested resource was not found.",
  "detail": "Post with pk=42 not found.",
  "message_key": "varco.error.not_found",
  "params": {"entity": "Post", "entity_id": "42"}
}
```

An out-of-tree `ServiceException` subclass that never sets `message_key`:

```json
{
  "code": "APP_001",
  "http_status": 429,
  "message": "Request quota exceeded.",
  "detail": "..."
}
```

Byte-identical to what it produced before this plan — `message_key` and
`params` are only ever added, never present-but-empty.

## `code` vs. `message_key` — two different stability contracts

| | `code` | `message_key` |
|---|---|---|
| Example | `FASTREST_001` | `varco.error.not_found` |
| What it identifies | The **machine** condition — alerting rules, log greps, exact-match client code | The **i18n** lookup key — a `MessageCatalog` (`technical_docs/features/i18n-and-localization.md`) entry |
| Stability contract | Never renamed after release (`codes.py`'s own docstring) | Never renamed after release, same rule |
| Namespaced / legible | No — historical `FASTREST_NNN` | Yes — dotted, human-legible |

`varco_core/varco_core/exception/codes.py:63` used to (incorrectly)
document `code` itself as "the i18n translation key" — that was wrong even
before this plan (`FastrestErrorCodes("FASTREST_001")` doesn't even
round-trip through the enum's own `_missing_`, a pre-existing documented
edge case) and is corrected by this plan. `message_key` is the field that
actually plays that role now.

### Why `FASTREST_*` is not renamed to `VARCO_*`

The backlog asked for prettier codes (`VARCO_001`). Renaming the *value* of
a code whose entire contract is "stable, never change it" is exactly the
change the contract forbids — any client keying an alert or a translation
table off `FASTREST_001` breaks silently (a renamed code produces a
valid-looking response with an unrecognized value). Instead:

- `VarcoErrorCodes` is a **bare alias to the identical enum object**
  (`VarcoErrorCodes = FastrestErrorCodes`), exported from `varco_core`. It
  is not a subclass, carries no `DeprecationWarning` (same treatment
  `JwtUtil.SYSTEM_ISSUER` got), and `isinstance`/identity/`list(...)`/every
  existing import keeps working — because it is the same class object, not
  a copy.
- The pretty, namespaced name people actually wanted is `message_key`
  (`varco.error.not_found`) — I1 is precisely the feature that adds it.

Rejected alternative: an `ErrorCodeStyle` setting emitting either
`FASTREST_001` or `VARCO_001` at runtime. ✅ opt-in nicer name. ❌ makes the
*stable identifier* configuration-dependent — two deployments of the same
framework version would emit different codes for the same condition, and a
shared client-side mapping table becomes ambiguous. Strictly worse than
either choice alone. Rejected.

## The `message_key` catalogue (built-in codes)

| `code` | `message_key` | HTTP |
|---|---|---|
| `FASTREST_001` | `varco.error.not_found` | 404 |
| `FASTREST_002` | `varco.error.unauthorized` | 403 |
| `FASTREST_003` | `varco.error.conflict` | 409 |
| `FASTREST_004` | `varco.error.validation_failed` | 422 |
| `FASTREST_500` | `varco.error.internal` | 500 |

## `error_params()` — structured interpolation data, with a hard no-secrets rule

Every built-in `ServiceException` subclass overrides `error_params()`
(base default: `{}`):

```python
class ServiceNotFoundError(ServiceException):
    message_key = "varco.error.not_found"

    def error_params(self) -> dict[str, Any]:
        return {"entity": self.entity_cls.__name__, "entity_id": self.entity_id}
```

**Deliberately excluded fields.** `ServiceAuthorizationError.error_params()`
does *not* include `reason` — that attribute is documented server-side-only
and must never reach a client. This is the rule to apply to any future
`error_params()` override: a `params` dict is exactly the kind of thing
someone later fills with `vars(exc)`, and that is precisely how an
authorization-denial reason, an internal entity ID format, or a stack-trace
fragment ends up in a public API response. Treat every field you add to
`params` as something a client will read and log — because it will.

`error_message_for(exc, message_resolver=...)`'s params flow:

```python
params = exc.error_params() if isinstance(exc, ServiceException) else {}
```

`params` is only ever emitted when non-empty (`if params`), same
byte-identical-by-default rule as `message_key`.

## `message_resolver` — the seam I2 plugs into

`error_message_for()` accepts an optional `message_resolver:
Callable[[str, Mapping[str, Any]], str | None]` — `(message_key, params) ->
rendered | None`. `None` means "no translation available", which falls back
to `translator`/`default_message` exactly as if no resolver were supplied —
a missing catalog entry can never produce an empty `message`. A resolver
that raises is caught and logged (rendering an error must never itself
raise) and treated identically to returning `None`.

`translator: Callable[[str], str] | None` (the pre-Plan-011 parameter) still
works unchanged, keyed on the stable `code` string, and is tried only after
`message_resolver` declines.

**Status note — this seam is now wired into both shipped HTTP error paths**
(Plan 011 drift-fix pass). Both `varco_fastapi.exceptions.
_make_error_response()` (used by `add_exception_handlers()`) and
`varco_fastapi.middleware.error.ErrorMiddleware` accept a `message_catalog=`
parameter and, when one is supplied (and a locale was resolved for the
request — see the RD-3 `request.state` mirror in
`technical_docs/features/timezone-handling.md`), construct a
`message_resolver=lambda key, params: message_catalog.format_message(key,
locale, params)` and pass it to `error_message_for()`. `create_varco_app()`
wires `message_catalog=` automatically from its resolved `MessageCatalog`
when I18n is enabled, so `message`/`title` are now genuinely
`GettextMessageCatalog`/`DictMessageCatalog`-rendered on the error path,
not just `default_message`. Both correctly pass/construct
`envelope_settings=ErrorEnvelopeSettings()` as before, so `message_key`/
`params` still appear on every built-in error body regardless. With no
`message_catalog=` supplied (i18n disabled, the default, or a handler you
wired yourself without it), the rendered string is `default_message` (or a
`translator=` result) exactly as before this fix — byte-identical.

## The RFC 9457 `problem+json` opt-in — and why it is not the default

```python
ErrorEnvelopeSettings(problem_details=True, problem_type_base="https://errors.example.com/")
# VARCO_ERROR_PROBLEM_DETAILS=true
```

Switches the media type to `application/problem+json` and adds
`type`/`title`/`status`/`instance` alongside the existing `code`/`message`/
`detail`/`message_key`/`params` shape. `type` is built from
`problem_type_base` + `message_key` (falls back to `about:blank` with
neither set). This is additive (nothing is removed under either mode) but
is still opt-in: switching a service's default media type is the kind of
change that breaks a strict-`Accept`-header client, and brief 003's own
migration guidance is explicit that this is the breaking move to gate
behind a flag, not a default.

`FieldError` (`varco_core.exception.http.FieldError`) is the shape reserved
for per-field validation detail (`ErrorMessage.errors: list[FieldError]`,
default `[]`) — the de-facto convention across Spring Boot 3, ASP.NET Core's
`ValidationProblemDetails`, and `fastapi-problem-details`. No built-in
`ServiceValidationError` populates it today; it is available for an app's
own multi-field validation handler.

## S3 — the unmapped-exception fallback no longer echoes `str(exc)`

Plan 035 (BACKLOG 3.2 / S3, §D-S3a). Two fallback sites — reached only when `error_message_for()`
itself raises (e.g. `exc.error_params()` raises), never by an unmapped `DBAPIError`/`OSError`
(those are caught earlier and rendered by the already-sanitized `_internal_error_response()`) —
used to put `str(exc)` directly into the response body:
`ErrorMiddleware._service_error_response`'s `except` branch (`middleware/error.py`) and
`add_exception_handlers`'s `_make_error_response`'s `except` branch (`exceptions.py`). Both now
return `{"code": ..., "message": "An internal error occurred."}` plus `correlation_id`, and log
the exception type server-side at ERROR with `exc_info=True`.

This is a **separate** knob from `ErrorEnvelopeSettings.include_detail` above — that one gates the
*mapped*-exception `detail` echo, kept because it is `RouteGuard`'s only actionable channel and
therefore a warn-only, 4.0-flip-candidate default. The S3 fix is unconditional and shipped in 3.2
because the only channel it closes is a body that was already failing to render properly.

## RD-7 — framework responsibility line

varco owns: the `message_key`/`code` taxonomy, the envelope shape, the
kill switch, and the `MessageCatalog` *seam* `message_resolver` plugs into.
varco does **not** own: message authoring, translation management, or
deciding what your `.mo` files say. See
`technical_docs/features/i18n-and-localization.md` for the catalog side.

## See also

- `technical_docs/features/i18n-and-localization.md` — `MessageCatalog`,
  the precedence chain, `LocalizationMiddleware`.
- `technical_docs/features/timezone-handling.md` — T1/T3, the sibling X1
  consumer.

## Pitfalls

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Error body gained `message_key`/`params` after upgrade** | An exact-equality assertion on an error response body fails after a version bump | Plan 011 / D-4 — the one deliberate wire delta: built-in varco exceptions now emit `message_key` (`varco.error.not_found`) and non-empty `params` as extension members. An out-of-tree `ServiceException` with no `message_key` is unaffected | Assert on the keys you care about instead of the whole dict, or restore the exact pre-plan body with `VARCO_ERROR_INCLUDE_MESSAGE_KEY=false` / `VARCO_ERROR_INCLUDE_PARAMS=false` |
| **The response no longer carries the exception string** | A previously-informative unmapped-exception body now reads `"An internal error occurred."` | Plan 035 / S3, §D-S3a — the fallback `str(exc)` echo was a genuine information leak and is fixed unconditionally in 3.2 (unlike `include_detail`, which is warn-only) | Correlate via the `correlation_id` in the response body against the server-side ERROR log line, which carries the exception type and `exc_info=True` |
| **Error response not localized although i18n is enabled** | A 404/500 body is in English (and has no `Content-Language` header) despite I18n being enabled and `?lang=fr` set | `create_varco_app()` only wires `message_catalog=` into the error paths when a `MessageCatalog` was actually resolved (`i18n.enabled=True` **and** a container was passed) — with no catalog bound, both `_make_error_response()`/`add_exception_handlers()` and `ErrorMiddleware` are byte-identical to before this fix: `message_key`/`params` still appear, but `message` stays `default_message` | Confirm `create_varco_app(container=..., i18n=I18nSettings(enabled=True))` and that a `MessageCatalog` (e.g. `GettextMessageCatalog`) is actually bound in the container; if you built a custom exception handler yourself, pass `message_catalog=`/`set_content_language=` explicitly — see `technical_docs/features/error-taxonomy-and-i18n.md`'s `message_resolver` section |
