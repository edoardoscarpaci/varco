"""
Plan 051 (DI-5) / §D-posture — RED tests: ``PostureSeverity``,
``PostureFinding``, ``PostureCollector`` move to ``varco_core.posture``
and ``PostureCollector`` becomes a ``@Multibound`` collection seam.

``varco_core/varco_core/posture.py`` does not exist yet (Step 4). Every
test here is expected to fail with ``ImportError`` until it lands.
"""

from __future__ import annotations

import pytest


def test_posture_finding_and_severity_importable_from_varco_core() -> None:
    from varco_core.posture import PostureFinding, PostureSeverity

    # PostureFinding moved verbatim (§D-posture) — its real fields are
    # check/severity/detail/remediation/suppressed, not `message` (there is
    # no such field, pre- or post-move).
    finding = PostureFinding(
        check="example.check",
        severity=PostureSeverity.INFO,
        detail="ok",
        remediation="no action required",
    )
    assert finding.check == "example.check"


def test_posture_collector_is_an_abstract_base_with_name_and_collect() -> None:
    from varco_core.posture import PostureCollector

    with pytest.raises(TypeError):
        PostureCollector()  # type: ignore[abstract]


def test_posture_collector_is_multibound_seam() -> None:
    from varco_core.posture import PostureCollector

    assert getattr(PostureCollector, "__di_multibound__", None) is not None


async def test_collect_posture_collector_includes_a_subclass_contribution() -> None:
    from providify import DIContainer, Singleton
    from varco_core.di.collect import collect
    from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity

    @Singleton
    class _MyCollector(PostureCollector):
        name = "my-collector"

        def collect(self) -> list[PostureFinding]:
            return [PostureFinding(check="my.check", severity=PostureSeverity.INFO, message="ok")]

    container = DIContainer()
    container.register(_MyCollector)

    result = collect(container, PostureCollector)
    assert len(result) == 1
    assert result[0].name == "my-collector"


def test_varco_core_lazy_map_exposes_posture_names() -> None:
    import varco_core

    assert varco_core.PostureCollector is not None
    assert varco_core.PostureFinding is not None
    assert varco_core.PostureSeverity is not None
