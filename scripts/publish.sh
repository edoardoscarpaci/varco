#!/usr/bin/env bash
# Usage:
#   scripts/publish.sh varco_beanie [extra poetry publish args]
#   scripts/publish.sh varco_sa [extra poetry publish args]
#
# Publishes a monorepo sub-package by temporarily rewriting any path
# dependencies on sibling packages to version constraints, building the
# wheel/sdist, publishing to PyPI, then restoring pyproject.toml.
set -euo pipefail

PACKAGE=${1:?Usage: scripts/publish.sh <package_dir> [poetry publish args]}
shift
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PKG_DIR="$ROOT/$PACKAGE"
PYPROJECT="$PKG_DIR/pyproject.toml"

if [[ ! -f "$PYPROJECT" ]]; then
  echo "ERROR: $PYPROJECT not found" >&2
  exit 1
fi

# Build replacement map: "package-name" -> ">=version" for each sibling
# Poetry project that appears as a path dep in the target pyproject.toml.
declare -A REPLACEMENTS
while IFS= read -r line; do
  # Match lines like: varco-core = {path = "../varco_core", develop = true}
  if [[ $line =~ ^([a-zA-Z0-9_-]+)[[:space:]]*=[[:space:]]*\{path[[:space:]]*=[[:space:]]*\"(\.\./[^\"]+)\" ]]; then
    DEP_NAME="${BASH_REMATCH[1]}"
    DEP_PATH="${BASH_REMATCH[2]}"
    SIBLING_DIR="$PKG_DIR/$DEP_PATH"
    if [[ -f "$SIBLING_DIR/pyproject.toml" ]]; then
      VERSION=$(cd "$SIBLING_DIR" && poetry version -s)
      REPLACEMENTS["$DEP_NAME"]="$VERSION"
    fi
  fi
done < "$PYPROJECT"

if [[ ${#REPLACEMENTS[@]} -eq 0 ]]; then
  echo "No path dependencies found — building and publishing directly."
  cd "$PKG_DIR"
  poetry build && poetry publish "$@"
  exit 0
fi

# Backup original pyproject.toml
cp "$PYPROJECT" "$PYPROJECT.bak"
cleanup() { mv "$PYPROJECT.bak" "$PYPROJECT"; }
trap cleanup EXIT

# Patch: replace each path dep with a semver constraint
for DEP_NAME in "${!REPLACEMENTS[@]}"; do
  VERSION="${REPLACEMENTS[$DEP_NAME]}"
  echo "  Rewriting $DEP_NAME  path dep  ->  >=$VERSION"
  # Matches: dep-name = {path = "../...", develop = true}  (with optional extras)
  sed -i "s|${DEP_NAME} = {path = \"[^\"]*\"[^}]*}|${DEP_NAME} = \">=${VERSION}\"|g" "$PYPROJECT"
done

echo ""
cd "$PKG_DIR"
poetry build && poetry publish "$@"
