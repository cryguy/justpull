//go:build !embedproxy

package main

// embeddedProxy reports that this build carries no proxy, as one made by go
// install does not.
func embeddedProxy(string) (gzipped []byte, ok bool) {
	return nil, false
}
