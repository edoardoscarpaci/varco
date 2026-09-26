"""
varco_core.settings
======================
The one recipe every varco settings class is registered through
(Plan 048 / DI-6, DI-16). See ``varco_core.settings.bind`` for the full
design rationale.

Public surface::

    from varco_core.settings import bind_settings, provenance_of
    from varco_core.settings import CommaSeparated
    from varco_core.settings import SettingsError, SettingsValidationError, SettingsAlreadyBoundError

``VarcoSettings`` itself still lives in ``varco_core.config`` and is
re-exported here purely so a settings module only needs one import line.

⚠️ **No module-level ``@Provider``/``@Singleton``/``@Configuration`` may
ever appear anywhere in this package** — ``container.scan("varco_core",
recursive=True)`` is a documented, in-use pattern (the same rule that
governs ``varco_core.tls``/``varco_core.event.cloudevents``), and a scanned
marker here would silently auto-activate on every such scan. Enforced by
``varco_core/tests/test_settings_bind.py::TestSettingsPackageHasNoScannedMarkers``.

Thread safety:  see ``bind.py``.
Async safety:   see ``bind.py``.
"""

from __future__ import annotations

from varco_core.config import VarcoSettings
from varco_core.settings.bind import SettingsProvenance, bind_settings, provenance_of
from varco_core.settings.errors import (
    SettingsAlreadyBoundError,
    SettingsError,
    SettingsValidationError,
)
from varco_core.settings.types import CommaSeparated

__all__ = [
    "CommaSeparated",
    "SettingsAlreadyBoundError",
    "SettingsError",
    "SettingsProvenance",
    "SettingsValidationError",
    "VarcoSettings",
    "bind_settings",
    "provenance_of",
]
