#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Builds and tests every commit of the Supermetal patch stack on its own, so each patch works
# without the patches after it. For each commit after BASE it runs clippy with warnings denied,
# then the unit tests and fork tests of the crates the commit touches.
#
# It checks out each commit in a temporary worktree and leaves the current checkout alone.
# Builds go to $CARGO_TARGET_DIR, or target/check-stack by default, and carry over between runs.
#
# Run it as `supermetal/check-stack.sh [BASE]`. BASE defaults to the merge base of HEAD and
# upstream/main and must be an ancestor of HEAD.

set -euo pipefail

base="${1:-$(git merge-base HEAD upstream/main)}"
if ! git rev-parse --quiet --verify "$base^{commit}" >/dev/null; then
  echo "BASE $base is not a commit" >&2
  exit 1
fi
if ! git merge-base --is-ancestor "$base" HEAD; then
  echo "BASE $base is not an ancestor of HEAD" >&2
  exit 1
fi
commits="$(git rev-list --reverse "$base"..HEAD)"
if [[ -z "$commits" ]]; then
  echo "There are no commits between $base and HEAD" >&2
  exit 1
fi

# A relative target directory would resolve inside the temporary worktree and vanish with it.
target_dir="${CARGO_TARGET_DIR:-$(git rev-parse --show-toplevel)/target/check-stack}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$PWD/$target_dir"
fi
export CARGO_TARGET_DIR="$target_dir"
worktree="$(mktemp -d)"
git worktree add --quiet --detach "$worktree" HEAD
trap 'git worktree remove --force "$worktree"' EXIT
cd "$worktree"

# Prints the directory of the crate that contains PATH, if any.
crate_dir() {
  local dir
  dir="$(dirname "$1")"
  while [[ "$dir" != "." && ! -f "$dir/Cargo.toml" ]]; do
    dir="$(dirname "$dir")"
  done
  if [[ "$dir" != "." ]]; then
    echo "$dir"
  fi
}

package_name() {
  sed -n 's/^name = "\(.*\)"$/\1/p' "$1/Cargo.toml" | head -1
}

for commit in $commits; do
  git checkout --quiet --detach "$commit"
  echo "== $(git log -1 --format='%h %s')"

  dirs="$(git diff --name-only "$commit~1" "$commit" -- crates | while read -r path; do
    if [[ -e "$path" ]]; then crate_dir "$path"; fi
  done | sort -u)"
  if [[ -z "$dirs" ]]; then
    echo "   Skipped because the commit changes no crate"
    continue
  fi

  packages=()
  for dir in $dirs; do
    packages+=(-p "$(package_name "$dir")")
  done
  cargo clippy --locked "${packages[@]}" --all-targets --all-features -- -D warnings

  for dir in $dirs; do
    targets=(--lib)
    for test in "$dir"/tests/supermetal*; do
      if [[ -e "$test" ]]; then targets+=(--test "$(basename "$test" .rs)"); fi
    done
    cargo test --locked -p "$(package_name "$dir")" --all-features "${targets[@]}"
  done
done
echo "All $(wc -w <<<"$commits") commits passed"
