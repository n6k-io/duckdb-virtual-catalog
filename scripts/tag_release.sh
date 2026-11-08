#!/usr/bin/env bash
# Tags origin/main HEAD with duckdb/v<VERSION> and python/v<pyproject version> where missing, then
# pushes them. Set DRY_RUN=1 to print the plan only.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

die() {
	echo "error: $*" >&2
	exit 1
}

gh auth status > /dev/null 2>&1 || die "gh is not authenticated (run: gh auth login)"

[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || die "not on main"
[[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"

git fetch --quiet origin main --tags
sha="$(git rev-parse HEAD)"
[[ "$sha" == "$(git rev-parse origin/main)" ]] || die "HEAD ($sha) is not origin/main"

run="$(gh api "repos/{owner}/{repo}/actions/workflows/MainDistributionPipeline.yml/runs?head_sha=$sha&event=push&branch=main" \
	--jq '.workflow_runs[0] | "\(.status) \(.conclusion) \(.html_url)"')"
[[ -n "$run" ]] || die "no MainDistributionPipeline push run for $sha"
read -r status conclusion url <<< "$run"
[[ "$status" == "completed" && "$conclusion" == "success" ]] \
	|| die "MainDistributionPipeline for $sha is $status/$conclusion: $url"

semver='^[0-9]+\.[0-9]+\.[0-9]+$'
duckdb_version="$(tr -d '[:space:]' < VERSION)"
python_version="$(python3 -c 'import tomllib; print(tomllib.load(open("python-virtual-catalog/pyproject.toml","rb"))["project"]["version"])')"
[[ "$duckdb_version" =~ $semver ]] || die "VERSION '$duckdb_version' is not x.y.z"
[[ "$python_version" =~ $semver ]] || die "pyproject version '$python_version' is not x.y.z"

remote_tag_sha() {
	git ls-remote origin "refs/tags/$1^{}" "refs/tags/$1" | awk 'END { print $1 }'
}

new_tags=()

# A tag elsewhere is fatal for duckdb (CI forces a VERSION bump per PR) but expected for python.
plan() {
	local tag="$1" on_other_commit="$2"
	local local_sha remote_sha
	local_sha="$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)"
	remote_sha="$(remote_tag_sha "$tag")"
	if [[ -n "$local_sha" && -n "$remote_sha" && "$local_sha" != "$remote_sha" ]]; then
		die "$tag differs locally ($local_sha) and on origin ($remote_sha)"
	fi
	local existing="${remote_sha:-$local_sha}"
	if [[ -z "$existing" ]]; then
		new_tags+=("$tag")
	elif [[ "$existing" == "$sha" ]]; then
		[[ -n "$remote_sha" ]] || new_tags+=("$tag")
		echo "$tag already on HEAD${remote_sha:+, skipping}"
	elif [[ "$on_other_commit" == "fail" ]]; then
		die "$tag already exists on $existing"
	else
		echo "$tag already exists on $existing, skipping"
	fi
}

plan "duckdb/v$duckdb_version" fail
plan "python/v$python_version" skip

if [[ ${#new_tags[@]} -eq 0 ]]; then
	echo "nothing to tag"
	exit 0
fi

echo "will tag $sha and push: ${new_tags[*]}"
if [[ "${DRY_RUN:-0}" == "1" ]]; then
	exit 0
fi

created=()
cleanup() {
	for tag in ${created[@]+"${created[@]}"}; do
		git tag -d "$tag" > /dev/null
	done
}
trap cleanup ERR

for tag in "${new_tags[@]}"; do
	if ! git rev-parse -q --verify "refs/tags/$tag" > /dev/null; then
		git tag -a "$tag" -m "$tag" "$sha"
		created+=("$tag")
	fi
done

git push --atomic origin "${new_tags[@]/#/refs/tags/}"
trap - ERR
echo "pushed: ${new_tags[*]}"
