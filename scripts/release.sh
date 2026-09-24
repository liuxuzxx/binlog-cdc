#!/usr/bin/env bash
set -euo pipefail

version="${1:-v1.0.0}"
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Usage: $0 vMAJOR.MINOR.PATCH" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

for command in cargo docker mktemp; do
  command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }
done
if [[ -z "${HTTPS_PROXY:-}" ]]; then
  docker_https_proxy="$(docker info --format '{{.HTTPSProxy}}')"
  if [[ -n "$docker_https_proxy" ]]; then
    export HTTPS_PROXY="$docker_https_proxy"
  fi
fi

expected_remote='https://github.com/liuxuzxx/binlog-cdc.git'
remote_url="$(git remote get-url origin)"
case "$remote_url" in
  "$expected_remote"|git@github.com:liuxuzxx/binlog-cdc.git) ;;
  *) echo "origin must point to liuxuzxx/binlog-cdc; got: $remote_url" >&2; exit 1 ;;
esac

if [[ -n "$(git status --porcelain)" ]]; then
  echo 'Commit all changes before releasing.' >&2
  exit 1
fi
if [[ "$(git branch --show-current)" != main ]]; then
  echo 'Release from the main branch.' >&2
  exit 1
fi
if [[ "$(git tag --list "$version")" != '' ]]; then
  echo "Local tag $version already exists." >&2
  exit 1
fi

git fetch origin main --tags
if ! git merge-base --is-ancestor origin/main HEAD; then
  echo 'Local main must contain origin/main. Rebase before releasing.' >&2
  exit 1
fi
if [[ -n "$(git ls-remote --tags origin "refs/tags/$version")" ]]; then
  echo "Remote tag $version already exists." >&2
  exit 1
fi
if ! git push --dry-run origin HEAD:main; then
  echo 'GitHub push check failed. Authenticate an account with write access to liuxuzxx/binlog-cdc.' >&2
  exit 1
fi

package_version="${version#v}"
for crate in flink-cdc-rs flink-cdc-init; do
  if ! grep -q "^version = \"$package_version\"$" "$crate/Cargo.toml"; then
    echo "$crate/Cargo.toml does not declare version $package_version." >&2
    exit 1
  fi
done
if [[ ! -f "docs/releases/$version.md" ]]; then
  echo "Missing docs/releases/$version.md" >&2
  exit 1
fi

commit_id="$(git rev-parse HEAD)"
commit_id="${commit_id:0:10}"
image="liuxuzxx/flink-cdc-rs:$version-$commit_id"
context_dir="$(mktemp -d)"
trap 'rm -rf "$context_dir"' EXIT

cargo build --locked --release -p flink-cdc-rs -p flink-cdc-init
install -m 755 target/release/flink-cdc-rs "$context_dir/flink-cdc-rs"
cp flink-cdc-rs/Dockerfile "$context_dir/Dockerfile"
docker build --pull -t "$image" "$context_dir"
docker push "$image"

git push origin HEAD:main
git tag -a "$version" -m "Release $version"
git push origin "refs/tags/$version"

echo "Docker Hub: docker.io/$image"
echo "GitHub Actions will build binaries and publish: https://github.com/liuxuzxx/binlog-cdc/releases/tag/$version"
