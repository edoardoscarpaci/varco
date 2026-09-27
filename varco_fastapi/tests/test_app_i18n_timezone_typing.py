"""
Regression tests — Plan 011, drift item 3.

User reports: ``create_varco_app(i18n=, timezone=)`` was typed ``Any | None``
with an ``isinstance(...) else <default>`` fallback — a typo'd or wrong-type
argument (e.g. a plain dict, or an unrelated object) was silently discarded
and replaced with the disabled default, so the caller's mistake produced no
error, only unexpectedly-disabled i18n/timezone. Correct behaviour: the
parameters are typed ``I18nSettings | None`` / ``TimezoneSettings | None``
(a type-checker error at the call site for a wrong type), and at runtime a
non-``None``, wrong-type value is used as given — surfacing as a legible
``AttributeError`` on first use (``.enabled``) rather than being silently
swallowed. ``None`` (the documented default sentinel) still resolves to
``I18nSettings()``/``TimezoneSettings()``, byte-identical to today (RD-1).
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.i18n.settings import I18nSettings
from varco_core.tz.settings import TimezoneSettings
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules import LocalizationModule
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter

# Plan 049 / DI-1 migration: `i18n=`/`timezone=` are now
# `LocalizationModule(i18n=, timezone=)` constructor arguments — the
# "used as given, fails loudly on first .enabled read" contract this file
# regression-tests is unchanged, just relocated from a create_varco_app
# keyword to the module that owns it.


class _PingRouter(GenericRouter):
    _prefix = "/ping"

    @route("GET", "")
    async def ping(self) -> dict:
        return {"ok": True}


def test_regression_wrong_type_i18n_no_longer_silently_falls_back() -> None:
    # Symptom: a wrong-type i18n= (e.g. a dict, or any non-I18nSettings
    # object) used to be silently discarded via isinstance(...) and replaced
    # with I18nSettings() — no error, no warning. Correct behaviour: the
    # bogus value is used as given and fails loudly the first time
    # `.enabled` is read.
    with pytest.raises(AttributeError):
        create_varco_app(
            DIContainer(),
            routers=[_PingRouter],
            modules=[LocalizationModule(i18n={"enabled": True})],  # wrong type
            validate=False,
        )


def test_regression_wrong_type_timezone_no_longer_silently_falls_back() -> None:
    with pytest.raises(AttributeError):
        create_varco_app(
            DIContainer(),
            routers=[_PingRouter],
            modules=[LocalizationModule(timezone=object())],  # wrong type
            validate=False,
        )


def test_none_i18n_and_timezone_still_resolve_to_defaults() -> None:
    # RD-1: the documented no-configuration behaviour is unchanged.
    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[LocalizationModule(i18n=None, timezone=None)],
        validate=False,
    )
    assert app is not None


def test_explicit_settings_instances_pass_through() -> None:
    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            LocalizationModule(
                i18n=I18nSettings(enabled=False),
                timezone=TimezoneSettings(enabled=False),
            )
        ],
        validate=False,
    )
    assert app is not None
