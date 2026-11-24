//go:build embedproxy

package main

import "embed"

// scripts/build.sh writes proxybin before it builds with -tags embedproxy.
//
//go:embed proxybin/justpull-linux-amd64.gz proxybin/justpull-linux-arm64.gz
var proxybin embed.FS

// embeddedProxy returns the gzipped linux/arch build of this program.
func embeddedProxy(arch string) (gzipped []byte, ok bool) {
	b, err := proxybin.ReadFile("proxybin/justpull-linux-" + arch + ".gz")
	if err != nil {
		return nil, false
	}
	return b, true
}
