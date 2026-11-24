//go:build !embedproxy

package main

import "testing"

func TestEmbeddedProxyNone(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		if gzipped, ok := embeddedProxy(arch); ok || gzipped != nil {
			t.Errorf("embeddedProxy(%q) = %d bytes, %v; want none without the embedproxy tag", arch, len(gzipped), ok)
		}
	}
}
