"""
Plan 058 / Step 1 — the deprecation floor (OQ3, §D-floor).

Invariant enforced over every ``since="X.Y.Z"`` / ``removed_in="X.Y.Z"`` pair
found in ``varco_*/varco_*/**/*.py`` (regex over source — no import, no
execution of any ``varco_*`` module):

1. ``removed_in`` is a major (``X.0.0``) strictly above ``major(since)``.
2. ``since`` is a release with a ``CHANGELOG.md`` heading, or the release
   being prepared (the pending workspace version, or the next major after it).
3. If ``removed_in`` is the release being prepared (or already has a heading),
   its date (heading date, or *today* when un-dated) is ``>= since_date + 12
   months`` (Feb 29 → Mar 1) — ``CONTRIBUTING.md`` §Removal window.
4. If ``removed_in``'s major is ``<=`` an already-released major, the symbol is
   overdue (or the label is stale).

Limitation: only *literal* keyword values are seen. A ``since=``/``removed_in=``
built from a variable is invisible to this test — never write one.

OQ3 answer (BACKLOG): "the major-version gate is necessary, not sufficient".

Red-first history: test (c) failed until Plan 058 / Step 2 re-labelled the nine
3.0.0-era sites from a 4.0.0 removal to 5.0.0 (their floor is 2027-08-31).
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

_HEADING_RE = re.compile(r"^## \[(\d+\.\d+\.\d+)\] — (\d{4})-(\d{2})-(\d{2})\s*$", re.M)
# One call: since=... then removed_in=... (or the reverse) with no paren between.
_SITE_RE = re.compile(
    r'since="(?P<s1>\d+\.\d+\.\d+)"[^()]*?removed_in="(?P<r1>\d+\.\d+\.\d+)"'
    r'|removed_in="(?P<r2>\d+\.\d+\.\d+)"[^()]*?since="(?P<s2>\d+\.\d+\.\d+)"',
    re.S,
)


@dataclass(frozen=True)
class Site:
    """One ``since=``/``removed_in=`` literal pair found in source.

    Attributes:
        path:       Repo-relative path of the file.
        line:       1-based line of the first keyword.
        since:      The ``since=`` version literal.
        removed_in: The ``removed_in=`` version literal.
    """

    path: str
    line: int
    since: str
    removed_in: str


def parse_release_dates(changelog_text: str) -> dict[str, date]:
    """Map every ``## [X.Y.Z] — YYYY-MM-DD`` heading to its date.

    Args:
        changelog_text: The full ``CHANGELOG.md`` text.

    Returns:
        ``{version: release_date}``; ``[Unreleased]`` never matches.
    """
    return {
        m.group(1): date(int(m.group(2)), int(m.group(3)), int(m.group(4)))
        for m in _HEADING_RE.finditer(changelog_text)
    }


def find_deprecation_sites(root: Path) -> list[Site]:
    """Regex-scan ``varco_*/varco_*/**/*.py`` for literal deprecation pairs.

    Args:
        root: The repository root.

    Returns:
        Every match, in path order. Non-literal keywords are invisible.
    """
    sites: list[Site] = []
    for pkg in sorted(root.glob("varco_*")):
        inner = pkg / pkg.name
        if not inner.is_dir():
            continue
        for py in sorted(inner.rglob("*.py")):
            text = py.read_text(encoding="utf-8")
            for m in _SITE_RE.finditer(text):
                sites.append(
                    Site(
                        path=str(py.relative_to(root)),
                        line=text.count("\n", 0, m.start()) + 1,
                        since=m.group("s1") or m.group("s2"),
                        removed_in=m.group("r1") or m.group("r2"),
                    )
                )
    return sites


def pending_version(root: Path) -> str:
    """Read the workspace version the way ``scripts/bump.py`` does.

    Args:
        root: The repository root.

    Returns:
        ``varco_core/pyproject.toml``'s ``[project] version``.
    """
    data = tomllib.loads((root / "varco_core" / "pyproject.toml").read_text())
    return str(data["project"]["version"])


def _v(s: str) -> tuple[int, int, int]:
    """Parse ``"X.Y.Z"`` into an int triple."""
    a, b, c = s.split(".")
    return int(a), int(b), int(c)


def _plus_one_year(d: date) -> date:
    """Return ``d`` + 12 months; Feb 29 maps to Mar 1 of the next year."""
    try:
        return d.replace(year=d.year + 1)
    except ValueError:  # Feb 29 -> Mar 1
        return date(d.year + 1, 3, 1)


def floor_violations(
    sites: list[Site],
    release_dates: dict[str, date],
    pending: str,
    *,
    today: date | None = None,
) -> list[str]:
    """Apply the four floor rules from the module docstring.

    Args:
        sites:         Pairs found by :func:`find_deprecation_sites`.
        release_dates: Output of :func:`parse_release_dates`.
        pending:       The workspace version (:func:`pending_version`).
        today:         Date standing in for an un-dated pending removal.

    Returns:
        One human-readable violation per offending site (empty = clean).
    """
    today = today or date.today()
    # The release being prepared: the pending version, and — while the tree is
    # still at a pre-bump minor (e.g. 3.2.0 during the 4.0.0 train) — the next
    # major. Once bumped to X.0.0, only X.0.0 itself.
    preparing = {pending}
    if _v(pending)[1:] != (0, 0):
        preparing.add(f"{_v(pending)[0] + 1}.0.0")
    released_majors = [
        _v(v)[0] for v in release_dates if v not in preparing and _v(v)[1:] == (0, 0)
    ]
    out: list[str] = []
    for s in sites:
        where = f"{s.path}:{s.line} (since={s.since}, removed_in={s.removed_in})"
        rmaj, rmin, rpat = _v(s.removed_in)
        if (rmin, rpat) != (0, 0):
            out.append(f"{where}: removed_in is not a major")
            continue
        if rmaj <= _v(s.since)[0]:
            out.append(f"{where}: removed_in major must exceed since major")
            continue
        if s.since not in release_dates and s.since not in preparing:
            out.append(f"{where}: unknown release {s.since}")
            continue
        if s.removed_in in release_dates and s.removed_in not in preparing:
            removal_date: date | None = release_dates[s.removed_in]
        elif s.removed_in in preparing:
            removal_date = release_dates.get(s.removed_in, today)
        else:
            removal_date = None
        if removal_date is not None and s.since in release_dates:
            floor = _plus_one_year(release_dates[s.since])
            if removal_date < floor:
                out.append(f"{where}: removal before 12-month floor {floor.isoformat()}")
                continue
        if released_majors and rmaj <= max(released_majors):
            out.append(f"{where}: overdue removal or stale label")
    return out


# ── (a) heading parser ───────────────────────────────────────────────────────


def test_known_changelog_headings_parse_to_known_dates() -> None:
    dates = parse_release_dates((REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))

    assert dates["3.0.0"] == date(2026, 8, 31)
    assert dates["3.1.0"] == date(2026, 9, 5)
    assert dates["3.2.0"] == date(2026, 9, 12)
    assert "Unreleased" not in dates


# ── (b) synthetic sites ──────────────────────────────────────────────────────

_DATES = {"3.0.0": date(2026, 8, 31), "3.1.0": date(2026, 9, 5), "3.2.0": date(2026, 9, 12)}
_TODAY = date(2026, 10, 6)


def _site(since: str, removed_in: str) -> Site:
    """Build a synthetic site at a fixed location."""
    return Site(path="pkg/mod.py", line=7, since=since, removed_in=removed_in)


def test_removal_in_pending_major_before_floor_is_a_violation_naming_the_site() -> None:
    out = floor_violations([_site("3.0.0", "4.0.0")], _DATES, "4.0.0", today=_TODAY)

    assert len(out) == 1
    assert "pkg/mod.py:7" in out[0]


def test_relabelled_to_next_major_is_clean() -> None:
    assert floor_violations([_site("3.0.0", "5.0.0")], _DATES, "4.0.0", today=_TODAY) == []


def test_minor_removed_in_is_a_violation() -> None:
    out = floor_violations([_site("3.0.0", "4.1.0")], _DATES, "4.0.0", today=_TODAY)
    assert out and "not a major" in out[0]


def test_removed_in_not_above_since_major_is_a_violation() -> None:
    assert floor_violations([_site("3.0.0", "3.0.0")], _DATES, "4.0.0", today=_TODAY)


def test_unknown_since_release_is_a_violation() -> None:
    out = floor_violations([_site("9.9.9", "10.0.0")], _DATES, "4.0.0", today=_TODAY)
    assert out and "unknown release" in out[0]


def test_since_equal_to_pending_is_allowed() -> None:
    assert floor_violations([_site("4.0.0", "5.0.0")], _DATES, "4.0.0", today=_TODAY) == []


def test_feb_29_since_floors_to_march_1() -> None:
    dates = {"1.0.0": date(2028, 2, 29), "2.0.0": date(2029, 2, 28)}
    out = floor_violations([_site("1.0.0", "2.0.0")], dates, "2.1.0", today=date(2029, 3, 5))
    assert out and "2029-03-01" in out[0]


def test_overdue_removal_after_released_major_is_a_violation() -> None:
    dates = {**_DATES, "4.0.0": date(2027, 9, 1)}
    out = floor_violations([_site("3.0.0", "4.0.0")], dates, "4.1.0", today=date(2027, 10, 1))
    assert out and "overdue" in out[0]


# ── (c)(d) the tree ──────────────────────────────────────────────────────────


def test_tree_has_no_deprecation_floor_violations() -> None:
    # Was red until Plan 058 / Step 2 re-labelled the 4.0.0 removals to 5.0.0.
    sites = find_deprecation_sites(REPO_ROOT)
    dates = parse_release_dates((REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))

    violations = floor_violations(sites, dates, pending_version(REPO_ROOT))

    assert violations == [], "\n".join(violations)


def test_site_regex_finds_at_least_nine_sites() -> None:
    # A regex that silently matches nothing is not a guard.
    assert len(find_deprecation_sites(REPO_ROOT)) >= 9


@pytest.mark.parametrize(
    "rel",
    [
        "varco_fastapi/varco_fastapi/auth/trust_store.py",
        "varco_core/varco_core/tenancy/control/consumer.py",
    ],
)
def test_converted_ad_hoc_sites_carry_floor_keywords(rel: str) -> None:
    # Step 2 (vi)/(vii): the two ad-hoc warnings.warn sites become warn_deprecated().
    assert any(s.path == rel for s in find_deprecation_sites(REPO_ROOT))


def test_no_direct_warnings_warn_deprecation_sites_remain() -> None:
    offenders = []
    for py in REPO_ROOT.glob("varco_*/varco_*/**/*.py"):
        if py.name == "deprecation.py":
            continue
        text = py.read_text(encoding="utf-8")
        for m in re.finditer(r"warnings\.warn\([^;]*?DeprecationWarning", text, re.S):
            offenders.append(f"{py.relative_to(REPO_ROOT)}:{text.count(chr(10), 0, m.start()) + 1}")
    assert offenders == [], offenders
