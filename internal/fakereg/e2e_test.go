//go:build e2e

package fakereg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestServe runs a registry for scripts/e2e.sh until stdin ends or SIGINT or
// SIGTERM arrives. It is configured by the environment, and skipped without
// FAKEREG_IMAGES:
//
//	FAKEREG_IMAGES     images, space-separated, each repo:tag=size[,size...]:
//	                   one random layer of each size in bytes
//	FAKEREG_FAULTS     faults, space-separated, each repo:tag/i=fault[,fault...]:
//	                   queued on the image's layer i (from 0), one per GET;
//	                   a fault is cut, reset, stall, corrupt, ignorerange,
//	                   wrongstart or deny
//	FAKEREG_USERNAME   the login, with FAKEREG_PASSWORD (Registry.Username)
//	FAKEREG_TOKEN_TTL  bearer tokens' lifetime, a Go duration (Registry.TokenTTL)
//	FAKEREG_REDIRECT   1 sends blob GETs to the storage host (Registry.Redirect)
//	FAKEREG_THROTTLE   blob bodies' bytes per second (Registry.Throttle)
//
// On stdout it prints one JSON line per image, {"image":"repo:tag",
// "digest":"sha256:...","config":"sha256:...","layers":[{"digest":
// "sha256:...","size":n}]}, then
// "FAKEREG <host:port>" once it serves, then one JSON line per request as it
// ends: {"host","method","path","range","status","bytes"}. The
// Authorization header is never printed.
func TestServe(t *testing.T) {
	images := os.Getenv("FAKEREG_IMAGES")
	if images == "" {
		t.Skip("FAKEREG_IMAGES is not set")
	}
	reg := New(t)
	reg.Username = os.Getenv("FAKEREG_USERNAME")
	reg.Password = os.Getenv("FAKEREG_PASSWORD")
	reg.Redirect = os.Getenv("FAKEREG_REDIRECT") == "1"
	if v := os.Getenv("FAKEREG_TOKEN_TTL"); v != "" {
		ttl, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("FAKEREG_TOKEN_TTL: %v", err)
		}
		reg.TokenTTL = ttl
	}
	if v := os.Getenv("FAKEREG_THROTTLE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("FAKEREG_THROTTLE: %v", err)
		}
		reg.Throttle = n
	}

	out := json.NewEncoder(os.Stdout)
	added := map[string]*Image{}
	for _, spec := range strings.Fields(images) {
		ref, list, ok := strings.Cut(spec, "=")
		repo, tag, tagged := strings.Cut(ref, ":")
		if !ok || !tagged {
			t.Fatalf("FAKEREG_IMAGES: %q is not repo:tag=size[,size...]", spec)
		}
		var sizes []int
		for _, s := range strings.Split(list, ",") {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 {
				t.Fatalf("FAKEREG_IMAGES: %q has a bad size %q", spec, s)
			}
			sizes = append(sizes, n)
		}
		img := reg.AddImage(repo, tag, sizes...)
		added[ref] = img
		type layer struct {
			Digest string `json:"digest"`
			Size   int    `json:"size"`
		}
		line := struct {
			Image  string  `json:"image"`
			Digest string  `json:"digest"`
			Config string  `json:"config"`
			Layers []layer `json:"layers"`
		}{Image: ref, Digest: img.Digest, Config: img.Config.Digest}
		for _, b := range img.Layers {
			line.Layers = append(line.Layers, layer{b.Digest, len(b.Data)})
		}
		_ = out.Encode(line)
	}

	for _, spec := range strings.Fields(os.Getenv("FAKEREG_FAULTS")) {
		target, list, ok := strings.Cut(spec, "=")
		i := strings.LastIndex(target, "/")
		if !ok || i < 0 {
			t.Fatalf("FAKEREG_FAULTS: %q is not repo:tag/i=fault[,fault...]", spec)
		}
		img := added[target[:i]]
		n, err := strconv.Atoi(target[i+1:])
		if img == nil || err != nil || n < 0 || n >= len(img.Layers) {
			t.Fatalf("FAKEREG_FAULTS: %q names no layer of FAKEREG_IMAGES", spec)
		}
		var faults []Fault
		for _, name := range strings.Split(list, ",") {
			f, ok := faultNames[name]
			if !ok {
				t.Fatalf("FAKEREG_FAULTS: %q has an unknown fault %q", spec, name)
			}
			faults = append(faults, f)
		}
		img.Layers[n].Faults(faults...)
	}

	fmt.Println("FAKEREG", reg.Host)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		stop()
	}()
	printed := 0
	printLog := func() {
		log := reg.Log()
		for _, r := range log[printed:] {
			_ = out.Encode(struct {
				Host   string `json:"host"`
				Method string `json:"method"`
				Path   string `json:"path"`
				Range  string `json:"range"`
				Status int    `json:"status"`
				Bytes  int64  `json:"bytes"`
			}{r.Host, r.Method, r.Path, r.Range, r.Status, r.Bytes})
		}
		printed = len(log)
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			printLog()
		case <-ctx.Done():
			printLog()
			return
		}
	}
}

var faultNames = map[string]Fault{
	"cut":         Cut,
	"reset":       Reset,
	"stall":       Stall,
	"corrupt":     Corrupt,
	"ignorerange": IgnoreRange,
	"wrongstart":  WrongStart,
	"deny":        Deny,
}
