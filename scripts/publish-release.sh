#!/usr/bin/env bash
set -euo pipefail

dist="${1:?usage: publish-release.sh DIST_DIRECTORY}"
tag="v${RELEASE_VERSION:?RELEASE_VERSION is required}"
commit="${RELEASE_COMMIT:?RELEASE_COMMIT is required}"
repository="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
default_branch="${GITHUB_DEFAULT_BRANCH:-$(gh repo view "$repository" --json defaultBranchRef --jq .defaultBranchRef.name)}"

git fetch origin "$default_branch" --tags --force
git cat-file -e "${commit}^{commit}"
git merge-base --is-ancestor "$commit" "origin/${default_branch}" || {
  echo "release commit $commit is not in origin/$default_branch" >&2
  exit 1
}
test "$(git show "$commit:Cargo.toml" | sed -n 's/^version = "\([^"]*\)"$/\1/p' | head -1)" = "$RELEASE_VERSION"

remote_tag="$(git ls-remote --tags origin "refs/tags/${tag}" | awk 'NR == 1 {print $1}')"
if [[ -z "$remote_tag" ]]; then
  git tag "$tag" "$commit"
  git push origin "refs/tags/${tag}"
else
  git fetch origin "refs/tags/${tag}:refs/tags/${tag}" --force
  tagged_commit="$(git rev-list -n 1 "$tag")"
  if [[ "$tagged_commit" != "$commit" ]]; then
    echo "existing tag $tag points at $tagged_commit, not $commit; refusing to move it" >&2
    exit 1
  fi
fi

mapfile -t local_assets < <(find "$dist" -maxdepth 1 -type f -print | sort)
if [[ "${#local_assets[@]}" -ne 6 ]]; then
  echo "expected exactly five archives and one checksum manifest, found ${#local_assets[@]}" >&2
  exit 1
fi

release_json="$(gh api --paginate --slurp "repos/${repository}/releases?per_page=100" \
  | jq -c --arg tag "$tag" '[.[][] | select(.tag_name == $tag)]')"
if [[ "$(jq length <<<"$release_json")" -gt 1 ]]; then
  echo "multiple GitHub Releases use tag $tag" >&2
  exit 1
fi
if [[ "$(jq length <<<"$release_json")" -eq 0 ]]; then
  release_json="$(gh api --method POST "repos/${repository}/releases" \
    -f tag_name="$tag" -f target_commitish="$commit" \
    -F draft=true -F generate_release_notes=true)"
else
  release_json="$(jq '.[0]' <<<"$release_json")"
fi
release_id="$(jq -r .id <<<"$release_json")"
is_draft="$(jq -r .draft <<<"$release_json")"
upload_url="$(jq -r .upload_url <<<"$release_json" | sed 's/{.*$//')"

upload_asset() {
  local asset="$1"
  local name
  name="$(basename "$asset")"
  curl --fail --silent --show-error --location --request POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GH_TOKEN:?GH_TOKEN is required}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@$asset" "${upload_url}?name=${name}" >/dev/null
}

verification="$(mktemp -d)"
trap 'rm -rf "$verification"' EXIT
for asset in "${local_assets[@]}"; do
  name="$(basename "$asset")"
  asset_id="$(jq -r --arg name "$name" '.assets[] | select(.name == $name) | .id' <<<"$release_json")"
  if [[ -n "$asset_id" ]]; then
    gh api -H "Accept: application/octet-stream" \
      "repos/${repository}/releases/assets/${asset_id}" >"$verification/$name"
    if cmp -s "$asset" "$verification/$name"; then
      continue
    fi
    if [[ "$is_draft" != true ]]; then
      echo "published release $tag has different bytes for $name; refusing to mutate it" >&2
      exit 1
    fi
    gh api --method DELETE "repos/${repository}/releases/assets/${asset_id}"
  fi
  if [[ "$is_draft" != true ]]; then
    echo "published release $tag is missing $name; refusing to mutate it" >&2
    exit 1
  fi
  upload_asset "$asset"
done

release_json="$(gh api "repos/${repository}/releases/${release_id}")"
mapfile -t expected_names < <(printf '%s\n' "${local_assets[@]##*/}" | sort)
while IFS=$'\t' read -r asset_id name; do
  if ! printf '%s\n' "${expected_names[@]}" | grep -Fxq "$name"; then
    if [[ "$is_draft" != true ]]; then
      echo "published release $tag has unexpected asset $name; refusing to mutate it" >&2
      exit 1
    fi
    gh api --method DELETE "repos/${repository}/releases/assets/${asset_id}"
  fi
done < <(jq -r '.assets[] | [.id, .name] | @tsv' <<<"$release_json")

release_json="$(gh api "repos/${repository}/releases/${release_id}")"
rm -f "$verification"/*
while IFS=$'\t' read -r asset_id name; do
  gh api -H "Accept: application/octet-stream" \
    "repos/${repository}/releases/assets/${asset_id}" >"$verification/$name"
done < <(jq -r '.assets[] | [.id, .name] | @tsv' <<<"$release_json")
for asset in "${local_assets[@]}"; do
  name="$(basename "$asset")"
  cmp "$asset" "$verification/$name" || {
    echo "remote release asset $name differs from the verified local bundle" >&2
    exit 1
  }
done
mapfile -t verified_names < <(find "$verification" -maxdepth 1 -type f -exec basename {} \; | sort)
if [[ "${verified_names[*]}" != "${expected_names[*]}" ]]; then
  echo "remote release asset set does not exactly match the bundle" >&2
  exit 1
fi

if [[ "$is_draft" == true ]]; then
  gh api --method PATCH "repos/${repository}/releases/${release_id}" -F draft=false >/dev/null
else
  echo "release $tag already exists with byte-identical assets"
fi
