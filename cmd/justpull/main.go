// Command justpull pulls container images through a cache that keeps every
// downloaded byte, so a broken pull continues where it stopped. For now it
// only has serve, which runs the proxy of one pull for a CLI on the host.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	// The container image is scratch, without a CA bundle. This embeds
	// Mozilla's roots, which Go uses only when the system has none.
	_ "golang.org/x/crypto/x509roots/fallback"
)

const usage = `usage: justpull serve [--cache DIR]

serve runs the proxy of one pull for the justpull CLI, which drives it over
stdin and stdout. DIR is the cache directory, /cache by default.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "serve" {
		return serve(context.Background(), args[1:], stdin, stdout, stderr)
	}
	fmt.Fprint(stderr, usage)
	return 2
}
