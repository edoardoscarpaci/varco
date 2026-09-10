"""
tests.test_redaction_extraction
=================================
Plan 040 / S21, Phase 1, Step 3 — the characterization test proving the
extraction of ``DEFAULT_REDACT_PATTERNS`` out of
``varco_core.observability.params`` and into ``varco_core.redaction.patterns``
changes NO behaviour: same object (identity, not equality), same public name,
and a one-way import direction (redaction never imports observability).
"""

from __future__ import annotations

import ast
from pathlib import Path

import varco_core.observability.params as params


def test_default_redact_patterns_is_the_same_object_across_both_paths() -> None:
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS as redaction_patterns

    # Object identity — not equality. The two modules must share one tuple.
    assert params.DEFAULT_REDACT_PATTERNS is redaction_patterns


def test_default_redact_patterns_equals_the_fifteen_literals_written_here() -> None:
    expected = (
        "password",
        "passwd",
        "secret",
        "token",
        "authorization",
        "auth",
        "api_key",
        "apikey",
        "credential",
        "private_key",
        "cookie",
        "session_id",
        "otp",
        "pin",
        "ssn",
    )
    assert params.DEFAULT_REDACT_PATTERNS == expected


def test_default_redact_patterns_still_exported_from_params_dunder_all() -> None:
    assert "DEFAULT_REDACT_PATTERNS" in params.__all__


def test_param_capture_config_default_redact_patterns_is_the_shared_object() -> None:
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS as redaction_patterns

    config = params.ParamCaptureConfig()
    assert config.redact_patterns is redaction_patterns


def test_redaction_package_imports_nothing_from_observability() -> None:
    """
    Import-direction guard (the ``test_tls_no_hard_client_deps.py`` shape):
    statically parse every module under ``varco_core/redaction`` and assert
    none of them import anything from ``varco_core.observability``.
    """
    import varco_core.redaction as redaction_pkg

    package_dir = Path(redaction_pkg.__file__).parent
    offending: list[str] = []

    for py_file in package_dir.rglob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("varco_core.observability"):
                        offending.append(f"{py_file}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("varco_core.observability"):
                    offending.append(f"{py_file}: from {module} import ...")

    assert offending == [], f"varco_core.redaction must not import observability: {offending}"
