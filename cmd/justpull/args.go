package main

import (
	"fmt"
	"strconv"
	"strings"
)

// The command line is docker pull's (docker/cli 29.8, cli/command/image/pull.go)
// parsed as its flag library does, with docker's messages naming justpull.
const (
	useLine = "justpull [OPTIONS] NAME[:TAG|@DIGEST]"
	short   = "Download an image from a registry, resuming broken downloads"
)

type pullOptions struct {
	remote   string
	all      bool
	platform string
	quiet    bool
	help     bool
	version  bool
}

// cmdError is a command line docker refuses: msg goes to stderr, and the
// command exits with code.
type cmdError struct {
	msg  string
	code int
}

// flagError is docker's answer to a flag its parser rejects.
func flagError(msg string) *cmdError {
	return &cmdError{fmt.Sprintf("%s\n\nUsage:  %s\n\nRun 'justpull --help' for more information", msg, useLine), 125}
}

// parseArgs reads args as docker pull does: flags anywhere, combined short
// flags, --name=value, and -- ending the flags. defaultPlatform is
// DOCKER_DEFAULT_PLATFORM. With help or version set, remote is not checked.
func parseArgs(args []string, defaultPlatform string) (pullOptions, *cmdError) {
	o := pullOptions{platform: defaultPlatform}
	var contentTrust bool // docker's deprecated --disable-content-trust, accepted and ignored
	bools := map[string]*bool{"all-tags": &o.all, "quiet": &o.quiet, "help": &o.help, "version": &o.version, "disable-content-trust": &contentTrust}
	shorts := map[byte]string{'a': "all-tags", 'q': "quiet", 'h': "help"}
	set := func(name, value string) *cmdError {
		if name == "platform" {
			o.platform = value
			return nil
		}
		v, err := strconv.ParseBool(value)
		if err != nil {
			return flagError(fmt.Sprintf("invalid argument %q for %q flag: %v", value, flagName(name, shorts), err))
		}
		*bools[name] = v
		return nil
	}

	var positional []string
	for i := 0; i < len(args); i++ {
		s := args[i]
		switch {
		case len(s) < 2 || s[0] != '-':
			positional = append(positional, s)
		case s == "--":
			positional = append(positional, args[i+1:]...)
			i = len(args)
		case s[1] == '-':
			if s[2] == '-' || s[2] == '=' {
				return o, flagError("bad flag syntax: " + s)
			}
			name, value, hasValue := strings.Cut(s[2:], "=")
			_, isBool := bools[name]
			switch {
			case !isBool && name != "platform":
				return o, flagError("unknown flag: --" + name)
			case hasValue:
			case isBool:
				value = "true"
			case i+1 < len(args):
				i++
				value = args[i]
			default:
				return o, flagError("flag needs an argument: --" + name)
			}
			if err := set(name, value); err != nil {
				return o, err
			}
		default:
			for shorthands := s[1:]; shorthands != ""; {
				name, ok := shorts[shorthands[0]]
				if !ok {
					return o, flagError(fmt.Sprintf("unknown shorthand flag: %q in -%s", rune(shorthands[0]), shorthands))
				}
				value, rest := "true", shorthands[1:]
				if len(shorthands) > 2 && shorthands[1] == '=' {
					value, rest = shorthands[2:], ""
				}
				if err := set(name, value); err != nil {
					return o, err
				}
				shorthands = rest
			}
		}
	}
	if o.help || o.version {
		return o, nil
	}
	if len(positional) != 1 {
		return o, &cmdError{fmt.Sprintf("justpull: 'justpull' requires 1 argument\n\nUsage:  %s\n\nRun 'justpull --help' for more information", useLine), 1}
	}
	o.remote = positional[0]
	return o, nil
}

// flagName names a flag in an invalid-value message. -h is docker's
// deprecated shorthand, which these messages leave out.
func flagName(name string, shorts map[byte]string) string {
	for c, long := range shorts {
		if long == name && c != 'h' {
			return fmt.Sprintf("-%c, --%s", c, name)
		}
	}
	return "--" + name
}

// helpText is docker pull --help's layout: each usage starts two columns
// after the longest flag.
func helpText(defaultPlatform string) string {
	platform := "Set platform if server is multi-platform capable"
	if defaultPlatform != "" {
		platform += fmt.Sprintf(" (default %q)", defaultPlatform)
	}
	flags := [][2]string{
		{"  -a, --all-tags", "Download all tagged images in the repository"},
		{"      --platform string", platform},
		{"  -q, --quiet", "Suppress verbose output"},
		{"      --version", "Print version information and quit"},
	}
	width := 0
	for _, f := range flags {
		width = max(width, len(f[0]))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Usage:  %s\n\n%s\n\nOptions:\n", useLine, short)
	for _, f := range flags {
		fmt.Fprintf(&b, "%-*s%s\n", width+3, f[0], f[1])
	}
	return b.String()
}
