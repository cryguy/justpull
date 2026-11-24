# justpull 🔁

`docker pull`, but it doesn't start over.

I got tired of watching `docker pull` fail halfway, dozens and dozens of
times, because of one small network blip. A blip like that should be
completely recoverable. Instead, Docker threw the progress away and
downloaded the whole huge layer again. 😩

justpull keeps every byte it already got. A broken download picks up right
where it stopped: inside one pull, after a failed attempt, even after a
restart.

```sh
justpull alpine:3.20
justpull --platform linux/arm64 ghcr.io/example/app@sha256:...
justpull -a example/app
```

It's a drop-in: same flags, same output, same exit codes as `docker pull`.
Your `docker login` just works.

## 📦 Get it

- **Grab a binary** from the
  [releases](https://github.com/cryguy/justpull/releases). That's all you need.
- **Or use Go:** `go install github.com/cryguy/justpull/cmd/justpull@latest`.
  The first run pulls the proxy image `ghcr.io/cryguy/justpull`.

You need Docker with Linux containers. It's been tested on Docker Desktop for
Windows and on Colima on macOS.

## ⚙️ How it works

No magic, just a tiny proxy. justpull starts it in a container and has Docker
pull through it. The proxy saves each layer into a cache volume,
`justpull-cache`, and streams it on to Docker as it arrives. When the
connection drops, the proxy reconnects and carries on from the last byte.
Docker never even notices.

A few things worth knowing:

- 🐢 **Slow is fine, stuck isn't.** If a layer gets no new byte for 10
  minutes, the pull gives up.
- ✅ **No half-baked layers.** Docker only gets a layer's last byte once its
  sha256 checks out.
- 🧹 **It cleans up.** After a good pull the cached files are gone. After a
  failed one they stick around for 24 hours, so the next try can use them.
- 💾 **Disk space:** while a pull runs, the layers sit on disk twice, once in
  the cache and once in Docker.
- 🔒 **Locked down.** The proxy runs as `nobody`, read-only, with no
  capabilities. Your login reaches it over stdin, never in its arguments or
  environment.
- 🌐 `HTTPS_PROXY` and `NO_PROXY` work.

Done with it? This removes everything:

```sh
docker volume rm justpull-cache
docker image rm $(docker image ls -q ghcr.io/cryguy/justpull)
```

## 🐹 Use it from Go

Want the same trick inside your own program? The library does the resuming;
you hand it three Docker calls.

```go
cache, err := justpull.New("/var/lib/example/pullcache", justpull.Options{})
if err != nil {
	return err
}
err = cache.Pull(ctx, dockerAdapter{cli}, "alpine:3.20", nil)
```

With `github.com/docker/docker/client`, the adapter is:

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

Logins, progress, sharing one cache between processes, and making the Docker
calls yourself are all in the
[package docs](https://pkg.go.dev/github.com/cryguy/justpull). It has one
dependency, `github.com/distribution/reference`.

⚠️ The library needs a Docker that can reach the machine's own `127.0.0.1`,
like stock Docker on Linux. Docker Desktop and Colima run Docker inside a VM,
which can't, so on those use the `justpull` command instead.

## 🛠️ Hacking on it

PRs are welcome! 🙌 Found a bug or have an idea? Open an issue or send a PR.
Before you send one, run the tests:

```sh
go test -race ./...   # Linux or macOS; no Docker or network needed
scripts/e2e.sh        # the real thing, against your Docker
```

`scripts/e2e.sh` needs the image `alpine:3.20`, and it deletes the
`justpull-cache` volume when it's done.

## 📄 License

Apache-2.0. See [LICENSE](LICENSE).
