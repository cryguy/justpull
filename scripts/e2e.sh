#!/usr/bin/env bash
# Runs justpull end to end against the Docker the docker command reaches.
# A release build of this checkout (scripts/build.sh e2e-<commit>) pulls
# from fake registries, internal/fakereg's TestServe in a container on the
# host's network at 127.0.0.1:<port>; each scenario checks justpull's
# output, the registry's request log and Docker's images, and prints PASS or
# FAIL. The script exits 1 when any scenario fails or anything is left over.
#
#   scripts/e2e.sh [scenario...]   all scenarios when none is named:
#                                  resume login wrong_password concurrent
#                                  all_tags digest
#
# Needs bash, Go, git, Docker with Linux containers, and the image
# alpine:3.20 already present: it looks into the justpull-cache volume.
# Nothing is pulled from the internet. Runs in Git Bash on Windows, and on
# Linux and macOS.
#
# Every exit, a failed one too, removes the run's containers (named
# justpull-e2e-<pid>-*), the images it made (127.0.0.1:<port>/e2e/*,
# justpull-e2e/*, ghcr.io/cryguy/justpull:e2e-*), the justpull-cache
# volume and its temporary directory, then lists any of those still there.
#
# Sourced, it only defines its functions.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
all_scenarios="resume login wrong_password concurrent all_tags digest"
fails=0
tmp=
run=justpull-e2e-$$

# hostpath prints a path as native programs read it: a Windows path in Git
# Bash.
hostpath() {
	if command -v cygpath >/dev/null 2>&1; then
		cygpath -w "$1"
	else
		printf '%s\n' "$1"
	fi
}

# setup builds justpull and the fake registry's image, and reads what the
# scenarios need from Docker.
setup() {
	cd "$root"
	tmp=$(mktemp -d)
	: >"$tmp/hosts"
	if ! docker image inspect alpine:3.20 >/dev/null 2>&1; then
		echo "e2e.sh: the image alpine:3.20 must be present; this script pulls nothing" >&2
		return 1
	fi
	# Read before a scenario points DOCKER_CONFIG at a directory without
	# Docker's contexts.
	docker_host=$(docker context inspect --format '{{.Endpoints.docker.Host}}')

	local sha arch exe=
	sha=$(git rev-parse --short HEAD)
	version=e2e-$sha
	fakereg_image=justpull-e2e/fakereg:$sha
	echo "== build $version"
	scripts/build.sh "$version" >/dev/null
	if [ "$(go env GOOS)" = windows ]; then
		exe=.exe
	fi
	cli=$root/dist/justpull-$(go env GOOS)-$(go env GOARCH)$exe

	arch=$(docker version --format '{{.Server.Arch}}')
	mkdir -p "$tmp/rootfs"
	CGO_ENABLED=0 GOOS=linux GOARCH=$arch go test -c -tags e2e -o "$tmp/rootfs/fakereg" ./internal/fakereg
	chmod 755 "$tmp/rootfs/fakereg"
	# Git Bash's chmod cannot mark an ELF file executable; GNU tar can.
	if tar --version 2>/dev/null | grep -q 'GNU tar'; then
		tar -C "$tmp/rootfs" --mode=0755 --owner=0 --group=0 -cf "$tmp/fakereg.tar" fakereg
	else
		tar -C "$tmp/rootfs" -cf "$tmp/fakereg.tar" fakereg
	fi
	MSYS_NO_PATHCONV=1 docker image import --platform "linux/$arch" \
		--change 'ENTRYPOINT ["/fakereg", "-test.run", "^TestServe$"]' \
		- "$fakereg_image" <"$tmp/fakereg.tar" >/dev/null
}

# start_fakereg NAME FAKEREG_X=value... runs a fake registry configured as
# TestServe documents and sets host to its address.
start_fakereg() {
	local name=$run-$1 kv i
	shift
	local args=()
	for kv in "$@"; do
		args+=(-e "$kv")
	done
	MSYS_NO_PATHCONV=1 docker run -d -i --rm --name "$name" --network host "${args[@]}" "$fakereg_image" >/dev/null
	for i in $(seq 150); do
		host=$(docker logs "$name" 2>/dev/null | tr -d '\r' | sed -n 's/^FAKEREG //p')
		if [ -n "$host" ]; then
			echo "$host" >>"$tmp/hosts"
			return 0
		fi
		sleep 0.2
	done
	echo "  the fake registry $name did not start:"
	docker logs "$name" 2>&1 | sed 's/^/  | /'
	return 1
}

# fakereg_log NAME prints the registry's stdout so far.
fakereg_log() {
	docker logs "$run-$1" 2>/dev/null | tr -d '\r'
}

# requests NAME prints the registry's requests as
# host|method|path|range|status|bytes. The registry prints a request once it
# ends, within 50 ms.
requests() {
	sleep 0.5
	fakereg_log "$1" | sed -n 's/^{"host":"\([^"]*\)","method":"\([^"]*\)","path":"\([^"]*\)","range":"\([^"]*\)","status":\([0-9]*\),"bytes":\([0-9]*\)}$/\1|\2|\3|\4|\5|\6/p'
}

# image_line NAME REPO:TAG prints the registry's line about an image.
image_line() {
	fakereg_log "$1" | grep -F "{\"image\":\"$2\","
}

# manifest_digest NAME REPO:TAG
manifest_digest() {
	image_line "$1" "$2" | sed 's/^{"image":"[^"]*","digest":"\([^"]*\)".*/\1/'
}

# config_digest NAME REPO:TAG
config_digest() {
	image_line "$1" "$2" | sed 's/^{"image":"[^"]*","digest":"[^"]*","config":"\([^"]*\)".*/\1/'
}

# layers NAME REPO:TAG prints "digest size" per layer.
layers() {
	image_line "$1" "$2" | grep -o '{"digest":"[^"]*","size":[0-9]*}' |
		sed 's/{"digest":"\([^"]*\)","size":\([0-9]*\)}/\1 \2/'
}

# blob_gets NAME DIGEST prints range|status|bytes of each GET of a blob.
blob_gets() {
	requests "$1" | awk -F'|' -v p="/blobs/$2" '$2 == "GET" && index($3, p) { print $4 "|" $5 "|" $6 }'
}

image_exists() {
	docker image inspect "$1" >/dev/null 2>&1
}

# e2e_names prints the names of the run's pulled images: repo:tag, or
# repo@digest for an image without a tag.
e2e_names() {
	docker image ls --digests --format '{{.Repository}} {{.Tag}} {{.Digest}}' |
		awk '$1 ~ /^127\.0\.0\.1:[0-9]+\/e2e\// { if ($2 == "<none>") print $1 "@" $3; else print $1 ":" $2 }' |
		sort -u
}

# strays prints the run's image names that are not under a fake registry's
# address: the proxy's local names, which justpull must remove.
strays() {
	local name h stray
	e2e_names | while read -r name; do
		stray=1
		while read -r h; do
			case $name in "$h"/*) stray= ;; esac
		done <"$tmp/hosts"
		if [ -n "$stray" ]; then
			echo "$name"
		fi
	done
}

# cache_list prints the paths in the justpull-cache volume, directories with
# a trailing slash.
cache_list() {
	MSYS_NO_PATHCONV=1 docker run --rm --name "$run-cache" --network none --pull never \
		-v justpull-cache:/c alpine:3.20 find /c -type d -exec printf '%s/\n' {} + -o -print | tr -d '\r'
}

# check DESCRIPTION COMMAND... runs a check and prints its outcome.
check() {
	local what=$1
	shift
	if "$@"; then
		echo "  ok: $what"
	else
		echo "  FAILED: $what"
		return 1
	fi
}

is() { [ "$1" = "$2" ]; }
empty() { [ -z "$1" ]; }

# no_match FILE... -- STRING... holds when no file contains any string.
no_match() {
	local files=() s
	while [ "$1" != -- ]; do
		files+=("$1")
		shift
	done
	shift
	for s in "$@"; do
		! grep -qF -e "$s" "${files[@]}" || return 1
	done
}

# own_addresses FILE... holds when the only 127.0.0.1 addresses in the files
# are the fake registries': docker would never print the proxy's.
own_addresses() {
	local other
	other=$(grep -ohE '127\.0\.0\.1:[0-9]+' "$@" | sort -u | grep -vxF -f "$tmp/hosts" || true)
	if [ -n "$other" ]; then
		echo "  the output shows $(echo $other)"
		return 1
	fi
}

# once SERVED SIZE holds when a blob of SIZE bytes was served about once:
# at least once, and under 1.5 times, which leaves room for resumed overlap.
once() {
	[ "$1" -ge "$2" ] && [ $(($1 * 2)) -lt $(($2 * 3)) ]
}

# show FILE... prints files indented.
show() {
	sed 's/^/  | /' "$@"
}

# justpull NAME ARGS... runs justpull with its stdout and stderr in
# $tmp/NAME.out and .err, and sets code to its exit code.
justpull() {
	local name=$1
	shift
	code=0
	"$cli" "$@" >"$tmp/$name.out" 2>"$tmp/$name.err" || code=$?
	echo "  \$ justpull $*   (exit $code)"
	show "$tmp/$name.out" "$tmp/$name.err"
}

# docker_config DIR REGISTRY USER:PASSWORD writes a docker config with that
# login.
docker_config() {
	mkdir -p "$1"
	printf '{"auths":{"%s":{"auth":"%s"}}}\n' "$2" "$(printf '%s' "$3" | base64)" >"$1/config.json"
}

secret() {
	printf 'pw-%s' "$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
}

# increasing OFFSET... holds when the first offset is 0 and every later one is
# larger than the one before.
increasing() {
	local prev o
	is "$1" 0 || return 1
	prev=0
	shift
	for o in "$@"; do
		[ "$o" -gt "$prev" ] || return 1
		prev=$o
	done
}

# A broken download resumes where it stopped, and the cache is emptied after
# the pull.
scenario_resume() {
	start_fakereg resume FAKEREG_IMAGES=e2e/resume:v1=20000000 FAKEREG_THROTTLE=4000000 \
		FAKEREG_FAULTS=e2e/resume:v1/0=cut,cut,cut || return 1
	local ref=$host/e2e/resume:v1 layer config manifest offsets gets
	justpull resume "$ref"
	layer=$(layers resume e2e/resume:v1 | cut -d' ' -f1)
	config=$(config_digest resume e2e/resume:v1)
	manifest=$(manifest_digest resume e2e/resume:v1)
	gets=$(blob_gets resume "$layer")
	echo "  layer GETs (range|status|bytes): $(echo $gets)"
	offsets=$(printf '%s\n' "$gets" | awk -F'|' '{ r = $1; sub(/^bytes=/, "", r); sub(/-.*/, "", r); print (r == "" ? 0 : r) }')
	check "justpull exits 0" is "$code" 0 || return 1
	check "its output shows Resuming at" grep -q 'Resuming at' "$tmp/resume.out" || return 1
	check "its output shows no address but the registry's" own_addresses "$tmp/resume.out" "$tmp/resume.err" || return 1
	# shellcheck disable=SC2086
	check "the layer was fetched from 0, then 3 times at increasing offsets" increasing $offsets || return 1
	check "the layer was fetched 4 times" is "$(printf '%s\n' "$offsets" | wc -l | tr -d ' ')" 4 || return 1
	check "$ref exists" image_exists "$ref" || return 1
	check "no local name is left" empty "$(strays)" || return 1
	local paths
	paths=$(cache_list) || {
		echo "  FAILED: could not list the justpull-cache volume"
		return 1
	}
	echo "  justpull-cache: $(echo $paths)"
	check "justpull-cache holds no file of the image" \
		empty "$(printf '%s\n' "$paths" | grep -e "${layer#sha256:}" -e "${config#sha256:}" -e "${manifest#sha256:}" || true)" || return 1
	check "justpull-cache holds no blob file" empty "$(printf '%s\n' "$paths" | grep '^/c/blobs/.*[^/]$' || true)" || return 1
}

# A private registry's login is read from the docker config and used.
scenario_login() {
	local user=e2e-user pass
	pass=$(secret)
	start_fakereg login FAKEREG_IMAGES=e2e/private:v1=300000 FAKEREG_USERNAME=$user \
		FAKEREG_PASSWORD="$pass" FAKEREG_TOKEN_TTL=30s || return 1
	docker_config "$tmp/config-login" "$host" "$user:$pass"
	local ref=$host/e2e/private:v1 tokens
	DOCKER_CONFIG=$(hostpath "$tmp/config-login") DOCKER_HOST=$docker_host justpull login "$ref"
	tokens=$(requests login | awk -F'|' '$1 == "token" { print $5 }' | sort | uniq -c | tr -s ' ' | tr '\n' ',')
	echo "  token requests (count status): $tokens"
	check "justpull exits 0" is "$code" 0 || return 1
	check "its output shows no address but the registry's" own_addresses "$tmp/login.out" "$tmp/login.err" || return 1
	check "the registry handed out a token for the login" grep -q '^token|GET|/token||200|' <(requests login) || return 1
	check "$ref exists" image_exists "$ref" || return 1
	check "no local name is left" empty "$(strays)" || return 1
}

# A refused login fails with the hint, and no password is printed anywhere.
scenario_wrong_password() {
	local user=e2e-user pass wrong
	pass=$(secret)
	wrong=$(secret)
	start_fakereg wrongpw FAKEREG_IMAGES=e2e/private:v1=300000 FAKEREG_USERNAME=$user \
		FAKEREG_PASSWORD="$pass" FAKEREG_TOKEN_TTL=30s || return 1
	docker_config "$tmp/config-wrong" "$host" "$user:$wrong"
	local ref=$host/e2e/private:v1 basic
	basic=$(printf '%s' "$user:$wrong" | base64)
	DOCKER_CONFIG=$(hostpath "$tmp/config-wrong") DOCKER_HOST=$docker_host justpull wrongpw "$ref"
	fakereg_log wrongpw >"$tmp/wrongpw.fakereg"
	check "justpull exits 1" is "$code" 1 || return 1
	check "stderr says the registry refused the login" grep -q '^justpull: .*refused the login' "$tmp/wrongpw.err" || return 1
	check "stderr has the hint" grep -qxF "log in with: docker login $host" "$tmp/wrongpw.err" || return 1
	check "its output shows no address but the registry's" own_addresses "$tmp/wrongpw.out" "$tmp/wrongpw.err" || return 1
	check "neither password, nor the Basic login sent, is in stdout, stderr or the registry's log" \
		no_match "$tmp/wrongpw.out" "$tmp/wrongpw.err" "$tmp/wrongpw.fakereg" -- "$pass" "$wrong" "$basic" || return 1
	check "no image was made" empty "$(e2e_names | grep -F "$host/" || true)" || return 1
}

# Two pulls of one image at once both succeed and fetch each layer once.
scenario_concurrent() {
	start_fakereg concurrent FAKEREG_IMAGES=e2e/shared:v1=8000000,4000000 FAKEREG_THROTTLE=2000000 || return 1
	local ref=$host/e2e/shared:v1 c1=0 c2=0 p1 p2 digest size served ok=0
	"$cli" "$ref" >"$tmp/c1.out" 2>"$tmp/c1.err" &
	p1=$!
	"$cli" "$ref" >"$tmp/c2.out" 2>"$tmp/c2.err" &
	p2=$!
	wait "$p1" || c1=$?
	wait "$p2" || c2=$?
	echo "  first justpull (exit $c1):"
	show "$tmp/c1.out" "$tmp/c1.err"
	echo "  second justpull (exit $c2):"
	show "$tmp/c2.out" "$tmp/c2.err"
	check "both exit 0" is "$c1/$c2" 0/0 || return 1
	check "their output shows no address but the registry's" own_addresses "$tmp"/c[12].out "$tmp"/c[12].err || return 1
	while read -r digest size; do
		served=$(blob_gets concurrent "$digest" | awk -F'|' '{ s += $3 } END { print s + 0 }')
		echo "  layer ${digest:7:12}: $size bytes, $served served in $(blob_gets concurrent "$digest" | wc -l | tr -d ' ') GETs"
		check "layer ${digest:7:12} was served once (at least 1x, under 1.5x its size)" \
			once "$served" "$size" || ok=1
	done < <(layers concurrent e2e/shared:v1)
	is "$ok" 0 || return 1
	check "$ref exists" image_exists "$ref" || return 1
	check "no local name is left" empty "$(strays)" || return 1
}

# -a pulls every tag and names each as the registry does.
scenario_all_tags() {
	start_fakereg tags 'FAKEREG_IMAGES=e2e/app:v1=200000 e2e/app:v2=300000' || return 1
	justpull tags -a "$host/e2e/app"
	check "justpull exits 0" is "$code" 0 || return 1
	check "its output shows no address but the registry's" own_addresses "$tmp/tags.out" "$tmp/tags.err" || return 1
	check "$host/e2e/app:v1 exists" image_exists "$host/e2e/app:v1" || return 1
	check "$host/e2e/app:v2 exists" image_exists "$host/e2e/app:v2" || return 1
	check "no local name is left" empty "$(strays)" || return 1
}

# A digest reference ends with the registry's name in RepoDigests.
scenario_digest() {
	start_fakereg digest FAKEREG_IMAGES=e2e/pinned:v1=250000 || return 1
	local ref repodigests
	ref=$host/e2e/pinned@$(manifest_digest digest e2e/pinned:v1)
	justpull digest "$ref"
	repodigests=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$ref" 2>&1 | tr -d '\r' || true)
	echo "  RepoDigests: $(echo $repodigests)"
	check "justpull exits 0" is "$code" 0 || return 1
	check "RepoDigests holds $ref" grep -qxF "$ref" <(printf '%s\n' "$repodigests") || return 1
	check "no local name is left" empty "$(strays)" || return 1
	check "its output shows no address but the registry's" own_addresses "$tmp/digest.out" "$tmp/digest.err" || return 1
}

scenario() {
	echo "== $1"
	if "scenario_$1"; then
		echo "PASS $1"
	else
		echo "FAIL $1"
		fails=$((fails + 1))
	fi
	docker rm -f "$run-$1" >/dev/null 2>&1 || true
}

# made_images prints the images the run makes.
made_images() {
	e2e_names
	docker image ls --format '{{.Repository}}:{{.Tag}}' | tr -d '\r' |
		grep -E '^(justpull-e2e/|ghcr\.io/cryguy/justpull:e2e-)' || true
}

# leftovers prints whatever cleanup should have removed.
leftovers() {
	docker ps -a --format '{{.Names}}' | tr -d '\r' | grep -E "^$run-" || true
	made_images
	docker volume ls -q | tr -d '\r' | grep -x justpull-cache || true
	if [ -n "$tmp" ] && [ -e "$tmp" ]; then
		echo "$tmp"
	fi
}

cleanup() {
	local status=$? c ref left
	set +e
	trap - EXIT
	echo "== cleanup"
	# An interrupted justpull may leave its proxy container for a moment.
	for c in $(docker ps -aq --filter "name=^$run-") ${version:+$(docker ps -aq --filter "ancestor=ghcr.io/cryguy/justpull:$version")}; do
		docker rm -f "$c" >/dev/null
	done
	for ref in $(made_images); do
		docker image rm "$ref" >/dev/null
	done
	if docker volume inspect justpull-cache >/dev/null 2>&1; then
		docker volume rm justpull-cache >/dev/null
	fi
	if [ -n "$tmp" ]; then
		rm -rf "$tmp"
	fi
	left=$(leftovers)
	if [ -n "$left" ]; then
		echo "LEFTOVER:"
		printf '%s\n' "$left" | sed 's/^/  /'
		status=1
	else
		echo "left over: none (no $run-* container, no e2e image, no justpull-cache volume, no $tmp)"
	fi
	exit "$status"
}

main() {
	trap cleanup EXIT
	trap 'exit 130' INT TERM
	local s scenarios=${*:-$all_scenarios}
	for s in $scenarios; do
		case " $all_scenarios " in
		*" $s "*) ;;
		*)
			echo "e2e.sh: unknown scenario $s; known: $all_scenarios" >&2
			exit 2
			;;
		esac
	done
	setup
	for s in $scenarios; do
		scenario "$s"
	done
	echo "== $fails of $(echo $scenarios | wc -w | tr -d ' ') scenarios failed"
	[ "$fails" = 0 ]
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
	main "$@"
fi
