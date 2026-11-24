# docker-cache-pull

A Go library that pulls container images so that a broken download picks up
where it stopped instead of starting over: inside one pull, across failed
attempts, and across restarts of the calling process.

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

Linux only. Pure Go; one dependency, `github.com/distribution/reference`.

## Use

```go
cache, err := dockercachepull.New("/var/lib/example/pullcache", dockercachepull.Options{})
if err != nil {
	return err
}
err = cache.Pull(ctx, dockerAdapter{cli}, "alpine:3.20", func(p dockercachepull.Progress) {
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

## Behaviour

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
- **Disk.** While a pull runs, the image's compressed layers are on disk twice:
  in the cache and in Docker.
- **Auth.** Anonymous pulls only: a bearer token is fetched when a registry
  challenges, as dockerd does without credentials.
- **Transport.** HTTP/1.1, so each layer has its own connection and a retry
  never rides a stuck one. Registries on loopback are plain HTTP, all others
  HTTPS. `HTTPS_PROXY` and `NO_PROXY` are honoured.
- **References.** A tag reference is pulled through the proxy and tagged back.
  A digest reference cannot be created with a tag, so after the proxy pull it is
  pulled again by its real name, which costs one manifest request because every
  layer is already in Docker.
- **The proxy** serves only the repository of the pull it belongs to and only
  `GET`/`HEAD`; everything else is `404` or `405`.

## Test

```sh
go test -race ./...
```

The tests need neither Docker nor the network. `internal/fakereg` is an
in-process registry with expiring tokens, storage redirects with expiring
links, and scripted faults per blob request (cut, reset, stall, corruption,
ignored `Range`, wrong start offset, `401` without a challenge). A fake Docker
pulls through the proxy the way dockerd does, including skipping manifests and
layers it already holds.
