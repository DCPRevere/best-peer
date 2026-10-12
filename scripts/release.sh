#!/usr/bin/env bash
# Cuts a release of best-peer: the version in Cargo.toml is the one source of truth, the tag `v<version>` on a
# commit of main is what publishes it (.github/workflows/release.yml builds, attests and publishes the binaries).
#
#   scripts/release.sh <version> -m "<what this release is, one or two sentences>"
#   scripts/release.sh 0.2.0 -m "The NVML adapter: a CUDA host says what it has and a deploy lands on the card."
#
# It refuses to run anywhere but a clean main that is level with origin, bumps the workspace version, refreshes the
# lockfile, commits "Release <version>", tags it with the message (annotated, so the message heads the release
# notes), and pushes main and the tag. The message is required: a release says what it is.
set -euo pipefail

usage() { sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

version=""; message=""
while [ $# -gt 0 ]; do
  case "$1" in
    -m) message="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) if [ -z "$version" ]; then version="$1"; shift; else echo "unexpected argument $1"; usage; fi ;;
  esac
done
[ -n "$version" ] || usage
[ -n "$message" ] || { echo "a release says what it is: pass -m \"...\""; exit 2; }
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || { echo "$version is not a semver version (MAJOR.MINOR.PATCH, optional -pre)"; exit 2; }

cd "$(git rev-parse --show-toplevel)"
[ "$(git rev-parse --abbrev-ref HEAD)" = main ] || { echo "releases are cut from main"; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "the tree is not clean"; exit 1; }
git fetch -q origin main
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || { echo "main is not level with origin/main: push (through the review gate) or pull first"; exit 1; }
git rev-parse -q --verify "refs/tags/v$version" > /dev/null && { echo "v$version already exists"; exit 1; }
current=$(grep -m1 -E '^version = "' Cargo.toml | sed -E 's/version = "(.*)"/\1/')
[ "$current" != "$version" ] || { echo "Cargo.toml already says $version; a release is a new version"; exit 1; }
latest=$(git tag --list 'v*' --sort=-version:refname | head -1)
[ -z "$latest" ] || echo "latest release: $latest"

# The one line under [workspace.package]; every crate inherits it. Perl, because sed differs between GNU and BSD.
CURRENT="$current" VERSION="$version" perl -pi -e 'if (!$done && s/^version = "\Q$ENV{CURRENT}\E"$/version = "$ENV{VERSION}"/) { $done = 1 }' Cargo.toml
grep -q "^version = \"$version\"$" Cargo.toml || { echo "could not set the version in Cargo.toml"; git checkout Cargo.toml; exit 1; }
cargo update --workspace --offline -q 2>/dev/null || cargo update --workspace -q
cargo build --release --locked -p best-peer -q
[ "$(./target/release/best-peer --version)" = "best-peer $version" ] || { echo "the binary does not say $version"; exit 1; }

git add Cargo.toml Cargo.lock
git commit -q -m "Release $version" -m "$message"
git tag -a "v$version" -m "$message"
git push origin main
git push origin "v$version"
echo "v$version pushed: https://github.com/behavioralstate/best-peer/actions/workflows/release.yml"
