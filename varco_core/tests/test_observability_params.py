"""
Unit tests for varco_core.observability.params (Plan 004 — Phase 1, deliverable A).

Covers the pure helpers used to auto-capture decorated-function arguments as
span attributes: ``sanitize_value`` (the value-rendering table), redaction,
``build_capture_plan`` / ``CapturePlan.extract`` (signature-introspection +
extraction), ``max_params`` truncation, and the env-var bootstrap for the
process-wide capture defaults.

No OTel SDK is needed here — these are pure-Python helpers with no tracer
dependency (see the plan's layer map: ``params.py`` imports only stdlib).

Global-state hygiene: ``set_capture_enabled`` / ``set_param_capture_defaults``
mutate process-wide module state, so every test resets it via the autouse
fixture below (Plan 004 "Risks" section).
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID

import pytest

# ── Global-state hygiene (mandatory per plan's Risks section) ──────────────


@pytest.fixture(autouse=True)
def _reset_param_capture_state():
    try:
        from varco_core.observability.params import reset_param_capture_state

        reset_param_capture_state()
    except ImportError:
        pass
    yield
    try:
        from varco_core.observability.params import reset_param_capture_state

        reset_param_capture_state()
    except ImportError:
        pass


# ── sanitize_value — rendering table ────────────────────────────────────────


class TestSanitizeValueScalars:
    def test_short_string_returned_unchanged(self) -> None:
        from varco_core.observability.params import sanitize_value

        assert sanitize_value("hello", max_value_length=256) == "hello"

    def test_long_string_truncated_with_ellipsis(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = "x" * 500
        result = sanitize_value(value, max_value_length=10)
        assert isinstance(result, str)
        assert len(result) <= 11  # 10 chars + ellipsis marker
        assert result.endswith("…")

    def test_int_kept_as_native_scalar(self) -> None:
        from varco_core.observability.params import sanitize_value

        result = sanitize_value(42, max_value_length=256)
        assert result == 42
        assert isinstance(result, int)

    def test_float_kept_as_native_scalar(self) -> None:
        from varco_core.observability.params import sanitize_value

        result = sanitize_value(3.14, max_value_length=256)
        assert result == 3.14
        assert isinstance(result, float)

    def test_bool_kept_as_native_scalar(self) -> None:
        from varco_core.observability.params import sanitize_value

        result = sanitize_value(True, max_value_length=256)
        assert result is True

    def test_none_renders_as_string_none(self) -> None:
        from varco_core.observability.params import sanitize_value

        assert sanitize_value(None, max_value_length=256) == "None"


class TestSanitizeValueStringableTypes:
    """UUID, Decimal, datetime, Enum, Path → str(value), truncated."""

    def test_uuid_rendered_as_str(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = UUID("12345678-1234-5678-1234-567812345678")
        assert sanitize_value(value, max_value_length=256) == str(value)

    def test_decimal_rendered_as_str(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = Decimal("19.99")
        assert sanitize_value(value, max_value_length=256) == "19.99"

    def test_datetime_rendered_as_str(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = datetime(2024, 1, 1, 12, 0, 0)
        assert sanitize_value(value, max_value_length=256) == str(value)

    def test_enum_rendered_as_str(self) -> None:
        from varco_core.observability.params import sanitize_value

        class Color(Enum):
            RED = "red"

        assert sanitize_value(Color.RED, max_value_length=256) == str(Color.RED)

    def test_path_rendered_as_str(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = Path("/tmp/foo")
        assert sanitize_value(value, max_value_length=256) == str(value)


class TestSanitizeValueSequences:
    def test_short_scalar_list_kept_as_native_sequence(self) -> None:
        from varco_core.observability.params import sanitize_value

        result = sanitize_value([1, 2, 3], max_value_length=256, max_sequence_items=10)
        assert result == (1, 2, 3) or result == [1, 2, 3]

    def test_long_sequence_summarised(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = list(range(1000))
        result = sanitize_value(value, max_value_length=256, max_sequence_items=10)
        assert isinstance(result, str)
        assert "1000" in result

    def test_mixed_type_sequence_summarised(self) -> None:
        from varco_core.observability.params import sanitize_value

        value = [1, "two", 3.0]
        result = sanitize_value(value, max_value_length=256, max_sequence_items=1)
        assert isinstance(result, str)


class TestSanitizeValueOpaqueObjects:
    def test_dict_summarised_by_type_name(self) -> None:
        from varco_core.observability.params import sanitize_value

        result = sanitize_value({"a": 1}, max_value_length=256)
        assert isinstance(result, str)
        assert "dict" in result

    def test_dataclass_summarised_by_type_name(self) -> None:
        from varco_core.observability.params import sanitize_value

        @dataclasses.dataclass
        class OrderCreateDTO:
            amount: int = 1

        result = sanitize_value(OrderCreateDTO(), max_value_length=256)
        assert isinstance(result, str)
        assert "OrderCreateDTO" in result

    def test_unrepresentable_object_does_not_raise(self) -> None:
        """An object whose __repr__/__str__ raises yields '<unrepresentable>'."""
        from varco_core.observability.params import sanitize_value

        class Boom:
            def __repr__(self) -> str:
                raise RuntimeError("no repr for you")

            def __str__(self) -> str:
                raise RuntimeError("no str for you")

        result = sanitize_value(Boom(), max_value_length=256)
        assert result == "<unrepresentable>"

    def test_repr_value_mode_uses_repr(self) -> None:
        from varco_core.observability.params import sanitize_value

        result = sanitize_value({"a": 1}, max_value_length=256, value_mode="repr")
        assert isinstance(result, str)
        assert "{" in result


# ── Redaction ────────────────────────────────────────────────────────────────


class TestRedaction:
    @pytest.mark.parametrize(
        "name",
        # NOTE: names must be valid Python identifiers — a real parameter can never be
        # literally "X-Authorization". "X_Authorization" covers the same contract:
        # case-insensitive WORD-BOUNDARY token match of the "authorization" pattern
        # (Plan 057 / FLIP-040-1 — the 4.0 default).
        ["password", "user_password", "X_Authorization", "api_key", "secret_token"],
    )
    def test_redact_patterns_match_by_word_case_insensitive(self, name: str) -> None:
        from varco_core.observability.params import build_capture_plan

        def fn(**kwargs):
            pass

        fn.__signature__ = None  # placeholder, replaced below via real func

        def real_fn(x=None):
            pass

        real_fn.__name__ = "real_fn"
        sig_src = f"def _f({name}=None): pass"
        ns: dict = {}
        exec(sig_src, ns)  # noqa: S102 — build a function with a dynamic param name
        target = ns["_f"]

        from varco_core.observability.params import ParamCaptureConfig

        plan = build_capture_plan(target, ParamCaptureConfig())
        extracted = plan.extract((), {name: "super-secret-value"})
        assert extracted.get(f"param.{name}") == "[REDACTED]"

    def test_shipping_address_param_not_redacted_by_default(self) -> None:
        # §D-span: span capture inherits the word default — 040's headline
        # false positive ("pin" in "shipping") is gone.
        from varco_core.observability.params import ParamCaptureConfig, build_capture_plan

        def fn(shipping_address=None):
            pass

        plan = build_capture_plan(fn, ParamCaptureConfig())
        extracted = plan.extract((), {"shipping_address": "1 Main St"})
        assert extracted.get("param.shipping_address") == "1 Main St"

    def test_redact_match_mode_substring_escape_hatch(self) -> None:
        # §D-span: a caller relying on substring semantics ("pwd" in "dbpwd")
        # says so on the ParamCaptureConfig it already holds.
        from varco_core.observability.params import ParamCaptureConfig, build_capture_plan

        def fn(dbpwd=None):
            pass

        substring = build_capture_plan(
            fn, ParamCaptureConfig(redact_patterns=("pwd",), redact_match_mode="substring")
        )
        assert substring.extract((), {"dbpwd": "x"}).get("param.dbpwd") == "[REDACTED]"
        default = build_capture_plan(fn, ParamCaptureConfig(redact_patterns=("pwd",)))
        assert default.extract((), {"dbpwd": "x"}).get("param.dbpwd") == "x"

    def test_redaction_wins_over_explicit_include(self) -> None:
        """Fail-closed: even an include-listed name is redacted if it matches a pattern."""
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(password=None):
            pass

        cfg = ParamCaptureConfig(include=("password",))
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((), {"password": "hunter2"})
        assert extracted.get("param.password") == "[REDACTED]"


# ── build_capture_plan / CapturePlan.extract ───────────────────────────────


class TestBuildCapturePlan:
    def test_skips_self_by_default(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        class Foo:
            def method(self, a):
                pass

        plan = build_capture_plan(Foo.method, ParamCaptureConfig())
        extracted = plan.extract((Foo(), 1), {})
        assert "param.self" not in extracted
        assert extracted.get("param.a") == 1

    def test_skips_cls_by_default(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def method(cls, a):
            pass

        plan = build_capture_plan(method, ParamCaptureConfig())
        extracted = plan.extract(("SomeClass", 1), {})
        assert "param.cls" not in extracted

    def test_capture_self_true_includes_self(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        class Foo:
            def method(self, a):
                pass

        cfg = ParamCaptureConfig(capture_self=True)
        plan = build_capture_plan(Foo.method, cfg)
        extracted = plan.extract((Foo(), 1), {})
        assert "param.self" in extracted

    def test_include_allow_list_restricts_capture(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, b, c):
            pass

        cfg = ParamCaptureConfig(include=("a",))
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((1, 2, 3), {})
        assert extracted == {"param.a": 1}

    def test_exclude_deny_list_applied_after_include(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, b):
            pass

        cfg = ParamCaptureConfig(exclude=("b",))
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((1, 2), {})
        assert extracted == {"param.a": 1}

    def test_varargs_skipped_by_default(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, *args, **kwargs):
            pass

        plan = build_capture_plan(fn, ParamCaptureConfig())
        extracted = plan.extract((1, 2, 3), {"x": "y"})
        assert extracted.get("param.a") == 1
        assert not any(k.startswith("param.args") for k in extracted)
        assert not any(k.startswith("param.x") for k in extracted)

    def test_varargs_captured_when_enabled(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, **kwargs):
            pass

        cfg = ParamCaptureConfig(capture_varargs=True)
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((1,), {"x": "y"})
        assert extracted.get("param.a") == 1
        assert extracted.get("param.x") == "y"

    def test_lambda_signature_failure_returns_empty_plan(self) -> None:
        """inspect.signature() raising must not blow up decoration — empty plan."""
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        target = min  # a builtin with no introspectable signature in many Pythons
        plan = build_capture_plan(target, ParamCaptureConfig())
        extracted = plan.extract((1, 2), {})
        assert extracted == {}

    def test_partial_of_uninspectable_callable_does_not_raise(self) -> None:
        import functools

        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        target = functools.partial(min)
        plan = build_capture_plan(target, ParamCaptureConfig())
        extracted = plan.extract((1, 2), {})
        assert isinstance(extracted, dict)

    def test_extra_positional_args_beyond_plan_are_ignored(self) -> None:
        """Wrong-arity calls must not make extract() raise before the callee does."""
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, b):
            pass

        plan = build_capture_plan(fn, ParamCaptureConfig())
        extracted = plan.extract((1, 2, 3, 4, 5), {})
        assert extracted.get("param.a") == 1
        assert extracted.get("param.b") == 2


class TestMaxParamsTruncation:
    def test_max_params_truncates_and_flags_truncated(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, b, c, d):
            pass

        cfg = ParamCaptureConfig(max_params=2)
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((1, 2, 3, 4), {})
        captured_params = {k: v for k, v in extracted.items() if k.startswith("param.")}
        assert len(captured_params) <= 2
        assert extracted.get("param._truncated") is True

    def test_max_params_zero_disables_capture_without_truncated_marker(self) -> None:
        """Edge case (plan): max_params=0 → no attributes, no _truncated noise."""
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a, b):
            pass

        cfg = ParamCaptureConfig(max_params=0)
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((1, 2), {})
        assert extracted == {}

    def test_no_truncated_marker_when_under_limit(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        def fn(a):
            pass

        cfg = ParamCaptureConfig(max_params=32)
        plan = build_capture_plan(fn, cfg)
        extracted = plan.extract((1,), {})
        assert "param._truncated" not in extracted


class TestExtractNeverRaises:
    def test_extract_swallows_sanitize_failure_and_returns_partial_dict(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            build_capture_plan,
        )

        class Boom:
            def __repr__(self) -> str:
                raise RuntimeError("boom")

            def __str__(self) -> str:
                raise RuntimeError("boom")

        def fn(a, b):
            pass

        plan = build_capture_plan(fn, ParamCaptureConfig())
        extracted = plan.extract((Boom(), 2), {})
        assert extracted.get("param.a") == "<unrepresentable>"
        assert extracted.get("param.b") == 2


# ── Enabling / disabling (process-wide kill switch) ─────────────────────────


class TestCaptureEnabledSwitch:
    def test_capture_enabled_defaults_true(self) -> None:
        from varco_core.observability.params import capture_enabled

        assert capture_enabled() is True

    def test_set_capture_enabled_false_disables_globally(self) -> None:
        from varco_core.observability.params import capture_enabled, set_capture_enabled

        set_capture_enabled(False)
        assert capture_enabled() is False

    def test_reset_param_capture_state_restores_default(self) -> None:
        from varco_core.observability.params import (
            capture_enabled,
            reset_param_capture_state,
            set_capture_enabled,
        )

        set_capture_enabled(False)
        reset_param_capture_state()
        assert capture_enabled() is True


# ── Env-var bootstrap ────────────────────────────────────────────────────────


class TestParamCaptureFromEnv:
    def test_capture_params_false_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from varco_core.observability.params import param_capture_from_env

        monkeypatch.setenv("VARCO_OTEL_CAPTURE_PARAMS", "false")
        cfg = param_capture_from_env()
        assert cfg.enabled is False

    def test_capture_params_true_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from varco_core.observability.params import param_capture_from_env

        monkeypatch.setenv("VARCO_OTEL_CAPTURE_PARAMS", "true")
        cfg = param_capture_from_env()
        assert cfg.enabled is True

    def test_invalid_capture_params_value_falls_back_to_default(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A malformed bool token must not crash the process — default + warning."""
        from varco_core.observability.params import param_capture_from_env

        monkeypatch.setenv("VARCO_OTEL_CAPTURE_PARAMS", "not-a-bool")
        with caplog.at_level("WARNING"):
            cfg = param_capture_from_env()
        assert cfg.enabled is True  # documented default

    def test_capture_params_exclude_parsed_as_tuple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from varco_core.observability.params import param_capture_from_env

        monkeypatch.setenv("VARCO_OTEL_CAPTURE_PARAMS_EXCLUDE", "field_a,field_b")
        cfg = param_capture_from_env()
        assert "field_a" in cfg.exclude
        assert "field_b" in cfg.exclude

    def test_unset_env_vars_yield_process_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from varco_core.observability.params import param_capture_from_env

        monkeypatch.delenv("VARCO_OTEL_CAPTURE_PARAMS", raising=False)
        monkeypatch.delenv("VARCO_OTEL_CAPTURE_PARAMS_EXCLUDE", raising=False)
        cfg = param_capture_from_env()
        assert cfg.enabled is True
        assert cfg.exclude == ()


class TestParamCaptureDefaults:
    def test_set_param_capture_defaults_overrides_process_config(self) -> None:
        from varco_core.observability.params import (
            ParamCaptureConfig,
            param_capture_defaults,
            set_param_capture_defaults,
        )

        set_param_capture_defaults(ParamCaptureConfig(max_params=5))
        assert param_capture_defaults().max_params == 5
