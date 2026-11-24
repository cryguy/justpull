package main

import (
	"regexp"
	"runtime/debug"
	"strings"
)

// version is set by scripts/build.sh. It is the proxy image's tag.
var version = "dev"

// pseudoVersion matches the end of a Go pseudo-version, which no release of
// the proxy image is tagged with.
var pseudoVersion = regexp.MustCompile(`[-.][0-9]{14}-[0-9a-f]{12}$`)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		version = buildVersion(version, info.Main.Version)
	}
}

// buildVersion keeps a version set at link time. Otherwise it takes the
// module version go install built, such as v1.2.3, so that the proxy image
// of that release is pulled. A local build's (devel), pseudo-version or
// +dirty version names no release, and stays dev.
func buildVersion(linked, module string) string {
	if linked != "dev" || module == "" || module == "(devel)" || strings.Contains(module, "+") || pseudoVersion.MatchString(module) {
		return linked
	}
	return module
}
