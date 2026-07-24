# Container image

The repository `Dockerfile` builds the `meld` CLI as a small non-root Linux
image. It is a standard binary delivery mechanism, not a hosted deployment
template: keep the API port private to the host or your application-owned TLS
proxy, mount data and secrets explicitly, and retain backup/restore receipts
outside the container lifecycle.

Build the exact checkout you intend to deploy:

```sh
docker build --tag meldbase:local .
docker run --rm meldbase:local --help
```

For a local, disposable smoke test, map the container process to your host
user so that the mounted state directory has deliberate ownership:

```sh
mkdir -p ./meldbase-container-data
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --read-only --tmpfs /tmp \
  --publish 127.0.0.1:8080:8080 \
  --volume "$(pwd)/meldbase-container-data:/var/lib/meldbase" \
  meldbase:local serve \
    --db /var/lib/meldbase/app.meld \
    --addr 0.0.0.0:8080 \
    --dev-no-auth
```

`--dev-no-auth` is intentionally limited to this local smoke test. For a
production single node, bind the published port only to the trusted proxy,
mount the JWT secret and access manifest read-only, and pass the equivalent
arguments shown in the [single-node deployment guide](/production-single-node).
For example, use `/run/secrets/jwt-hs256.secret` and
`/etc/meldbase/access-policy.json` as the container paths for
`--jwt-hs256-secret-file` and `--access-policy-file`; do not place either value
in an image layer or an environment variable. Persist `/var/lib/meldbase`, and
mount a separate protected backup target before enabling scheduled backups.

The image has no shell or package manager. Use `meld verify`, backup receipts,
and the host/container logging platform for diagnosis rather than modifying a
running container.
