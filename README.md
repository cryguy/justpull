# justpull

`docker pull`, except that a broken download picks up where it stopped
instead of starting over: inside one pull, across failed attempts, and across
restarts. It is a command, `justpull`, and the Go library behind it.

## The command

```sh
justpull alpine:3.20
justpull -q --platform linux/arm64 ghcr.io/example/app@sha256:...
justpull --all-tags example/app
```

It takes the same options as `docker pull` (`-a`, `--platform` with
`DOCKER_DEFAULT_PLATFORM`, `-q`), prints the same output and errors, and
exits with the same codes. It uses the login `docker login` stored. When a
registry refuses the login or the image, it says so and how to log in.

**Install.** Download a binary from the releases; it carries the proxy and
needs nothing else. Or `go install github.com/cryguy/justpull/cmd/justpull@latest`,
which pulls the proxy image `ghcr.io/cryguy/justpull:<version>` on its first
run.

**How it works.** Each pull starts the proxy below in a container on the
host's network, as `nobody`, without capabilities, read-only, with one
volume, `justpull-cache`, for its files. Docker pulls through it; the image
then gets the name you asked for. After a successful pull the image's files
are deleted from the volume. After a failed or interrupted one they stay for
24 hours, and the next pull of an image sharing those layers continues from
them. The login reaches the container on its stdin, never in its arguments
or environment.

**Requirements.** Docker with Linux containers. Docker Desktop on Windows was
tested; Docker Desktop on macOS was not.

**Remove everything.**

```sh
docker volume rm justpull-cache
docker image rm $(docker image ls -q ghcr.io/cryguy/justpull)
```

## The library

Docker is not replaced. Each pull starts a small read-only registry on
`127.0.0.1:<random port>` and asks Docker to pull
`127.0.0.1:<port>/<repository>:<tag>` from it. dockerd treats loopback
registries as insecure by default, so no daemon configuration is needed.
Docker asks only for the blobs it does not have, on both the classic and the
containerd image store. The proxy fetches those blobs from the real registry
into a cache on disk, resuming after any break, and streams them to Docker
while they download, so Docker's own connection never breaks. After a
successful pull the image is tagged with its real name and the localhost name
is removed.

The library's cache needs Unix (see Shared cache below); the command runs on
Windows, macOS and Linux, since its proxy runs in a Linux container. Pure Go;
the library has one dependency, `github.com/distribution/reference`, and
`cmd/justpull` adds `golang.org/x/crypto/x509roots/fallback`, `golang.org/x/term`
and `golang.org/x/sys`.

### Use

```go
cache, err := justpull.New("/var/lib/example/pullcache", justpull.Options{})
if err != nil {
	return err
}
err = cache.Pull(ctx, dockerAdapter{cli}, "alpine:3.20", func(p justpull.Progress) {
	// p.Done and p.Total are compressed bytes of the layers Docker is downloading.
})
```

`Pull` needs three Docker calls, given as the `Docker` interface so the
library does not depend on a Docker client version. Over
`github.com/docker/docker/client`:

```go
type dockerAdapter struct{ cli *client.Client }

func (d dockerAdapter) Pull(ctx context.Context, ref string) (io.ReadCloser, error) {
	return d.cli.ImagePull(ctx, ref, image.PullOptions{})
}

func (d dockerAdapter) Tag(ctx context.Context, source, target string) error {
	return d.cli.ImageTag(ctx, source, target)
}

func (d dockerAdapter) Untag(ctx context.Context, ref string) error {
	_, err := d.cli.ImageRemove(ctx, ref, image.RemoveOptions{})
	return err
}
```

`Pull` reads Docker's progress stream itself and returns the error Docker
reports inside it. Call `cache.Sweep()` periodically (for example hourly) in a
long-running process: `Pull` sweeps when it starts, but files of a cancelled
pull should also go when no new pull comes.

For private registries, set `Options.Credentials`. It gets the registry as the
reference names it (`docker.io`, `ghcr.io`, `localhost:5000`) and returns a
`Credential`, a username and password or an identity token, as `docker login`
stores them:

```go
justpull.Options{Credentials: func(ctx context.Context, registry string) (justpull.Credential, error) {
	return lookupLogin(registry) // a zero Credential pulls anonymously
}}
```

To make the Docker calls yourself, `cache.Serve(ctx, ref, justpull.SessionOptions{})`
starts the proxy of one pull and returns a `Session`; `Pull` is built on it.
Docker pulls `s.LocalRef()`. `s.Context()` ends, with the failure as its cause,
when a layer fails for good; cancel Docker's pull then. Tag the image and end
with `s.Finish(success)`, which deletes the image's cached files only on
success.

`cmd/justpull serve` runs a session over stdin and stdout for the justpull CLI.

### Behaviour

- **Resume.** Each layer is written to `<dir>/blobs/sha256/<hex>.partial` as it
  arrives and renamed to `<hex>` once its sha256 matches. A broken or quiet
  connection (no byte for 30 seconds) is closed and retried with
  `Range: bytes=<offset>-`, with backoff from 1 to 30 seconds. A partial file
  left by an earlier attempt is re-hashed and continued.
- **Stall limit.** A layer that receives no new byte for 10 minutes
  (`Options.StallLimit`), reconnects included, fails the pull. A slow link that
  keeps moving is never cut off. The proxy then cancels Docker's pull itself,
  so the limit is not multiplied by Docker's own retries.
- **Failures that are not retried:** a digest mismatch (the file is deleted),
  a registry 4xx such as `401` without a challenge or `404`, and a layer size
  that disagrees with the manifest.
- **Registry links.** Every attempt starts at the registry, never at a storage
  link handed out earlier, which may have expired. The registry's token is never
  sent to the storage host. Redirects are followed by the library; a refusal
  from the storage host is retried through the registry.
- **Servers that ignore `Range`** send the whole blob again; the bytes already
  on disk are skipped, so the file only ever grows. A `206` that starts at the
  wrong offset is retried.
- **Serving Docker.** Blobs are streamed from the cache file as it grows and
  Docker's own `Range` requests are honoured. The last byte is held back until
  the digest is verified, so Docker never receives a whole blob that turns out
  to be corrupt.
- **After a pull.** On success every cached file of the image is deleted, the
  blobs and the saved manifests alike. On failure or cancel they stay, and the
  next `Pull` of an image sharing those layers continues from them. `Sweep`
  deletes files not written for 24 hours (`Options.MaxAge`).
- **Shared cache.** Several processes, or several `Cache` values, may use one
  directory at once. A layer is downloaded once: the other pulls stream it to
  their Docker from the cache file as it arrives, and if the downloading
  process dies or gives up, one of them takes the download over from the bytes
  on disk. If that download fails its digest check, the pulls streaming it fail
  too, since their Docker already got the bad bytes. Cleanup after a pull and
  `Sweep` skip layers another process is using. The processes coordinate with
  `flock(2)` on files in `<dir>/locks/sha256/`, so this needs Unix (Linux,
  macOS, the BSDs); elsewhere `New` fails.
- **Disk.** While a pull runs, the image's compressed layers are on disk twice:
  in the cache and in Docker.
- **Auth.** `Options.Credentials` is asked only when a registry answers `401`
  with a challenge. A Basic challenge gets the username and password; a bearer
  challenge gets a token, fetched with the username and password or exchanged
  for the identity token (an OAuth2 refresh token), or anonymous without a
  login, as dockerd does. A login is never sent to a storage host, nor over
  plain HTTP to anything but loopback. A refused login, or a registry that
  still refuses the image, fails the pull at once with the registry's answer.
- **Transport.** HTTP/1.1, so each layer has its own connection and a retry
  never rides a stuck one. Registries on loopback are plain HTTP, all others
  HTTPS. `HTTPS_PROXY` and `NO_PROXY` are honoured.
- **References.** A tag reference is pulled through the proxy and tagged back.
  A digest reference cannot be created with a tag, so after the proxy pull it is
  pulled again by its real name, which costs one manifest request because every
  layer is already in Docker.
- **All tags.** With `SessionOptions{AllTags: true}` the reference names a
  repository only and `LocalRef` has no tag, for `docker pull --all-tags`. The
  proxy relays the repository's tag list page by page with Docker's `n` and
  `last`; the `Link` to the next page is rewritten to the proxy's own path, so
  Docker never asks the registry directly.
- **The proxy** serves only the repository of the pull it belongs to and only
  `GET`/`HEAD`; everything else is `404` or `405`.

## Test

```sh
go test -race ./...
```

The tests need neither Docker nor the network. `internal/fakereg` is an
in-process registry with expiring tokens, an optional login (Basic, or for the
token with a password or a refresh token), storage redirects with expiring
links, and scripted faults per blob request (cut, reset, stall, corruption,
ignored `Range`, wrong start offset, `401` without a challenge).
`internal/fakedocker` pulls through the proxy the way dockerd does, including
skipping manifests and layers it already holds and pulling every tag of a
repository named without one.

## License

Apache-2.0. See [LICENSE](LICENSE).
