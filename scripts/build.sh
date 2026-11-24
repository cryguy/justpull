#!/bin/sh
# Builds the release artifacts of justpull VERSION into dist/:
#   justpull-<os>-<arch>[.exe]   the CLIs, each embedding both Linux proxies
#   SHA256SUMS                   over the CLIs
#   proxy/justpull-linux-<arch>  the proxies, for Dockerfile.proxy
#   rootfs/cache/                the empty /cache of Dockerfile.proxy
# VERSION becomes main.version, which the CLI uses as the proxy image's tag,
# so it must be a valid Docker tag.
set -eu

if [ $# -ne 1 ]; then
	echo "usage: scripts/build.sh VERSION" >&2
	exit 2
fi
version=$1
case $version in
'' | [.-]* | *[!A-Za-z0-9_.-]*)
	echo "build.sh: VERSION must be a Docker tag ([A-Za-z0-9_][A-Za-z0-9_.-]*), got '$version'" >&2
	exit 2
	;;
esac

cd "$(dirname "$0")/.."
ldflags="-s -w -X main.version=$version"

rm -rf dist cmd/justpull/proxybin
mkdir -p dist/proxy dist/rootfs/cache cmd/justpull/proxybin

for arch in amd64 arm64; do
	out=dist/proxy/justpull-linux-$arch
	CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -ldflags "$ldflags" -o "$out" ./cmd/justpull
	gzip -9 -n -c "$out" >"cmd/justpull/proxybin/justpull-linux-$arch.gz"
done

for os in windows darwin linux; do
	for arch in amd64 arm64; do
		out=dist/justpull-$os-$arch
		if [ "$os" = windows ]; then
			out=$out.exe
		fi
		CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -tags embedproxy -ldflags "$ldflags" -o "$out" ./cmd/justpull
	done
done

(cd dist && sha256sum justpull-* >SHA256SUMS)

for f in dist/proxy/* cmd/justpull/proxybin/* dist/justpull-* dist/SHA256SUMS; do
	printf '%10d  %s\n' "$(wc -c <"$f")" "$f"
done
