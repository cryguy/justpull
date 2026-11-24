// Command justpull is docker pull through a cache that keeps every downloaded
// byte, so a broken pull continues where it stopped. justpull --serve runs
// the proxy of one pull in a container, driven by the CLI on the host.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	// The container image is scratch, without a CA bundle. This embeds
	// Mozilla's roots, which Go uses only when the system has none.
	_ "golang.org/x/crypto/x509roots/fallback"
)

const usage = `usage: justpull --serve [--cache DIR]

--serve runs the proxy of one pull for the justpull CLI, which drives it
over stdin and stdout. DIR is the cache directory, /cache by default.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	// A flag, unlike a word, can never be an image name, so justpull serve
	// pulls the image serve as docker pull does. It is not in the pull
	// command's options, so --help shows docker pull's usage unchanged.
	if len(args) > 0 && args[0] == "--serve" {
		// docker run forwards these to the container.
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return serve(ctx, args[1:], stdin, stdout, stderr)
	}
	ctx, stop := interruptContext(stderr)
	defer stop()
	var tty bool
	var width int
	if f, ok := stdout.(*os.File); ok {
		tty, width = terminal(f)
	}
	return pullCommand(ctx, args, &env{
		stdout: stdout,
		stderr: stderr,
		tty:    tty,
		width:  width,
		getenv: os.Getenv,
		config: loadConfig,
		docker: newDocker,
	})
}

// interruptContext ends on the first of docker's termination signals. The
// third exits at once, as docker does (cmd/docker/docker.go).
func interruptContext(stderr io.Writer) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 3)
	signal.Notify(signals, terminationSignals...)
	go func() {
		<-signals
		cancel()
		<-signals
		<-signals
		fmt.Fprint(stderr, "\ngot 3 SIGTERM/SIGINTs, forcefully exiting\n")
		os.Exit(1)
	}()
	return ctx, func() {
		signal.Stop(signals)
		cancel()
	}
}
