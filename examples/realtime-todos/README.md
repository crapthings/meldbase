# Realtime todos

This browser example uses the same data-only query through `@meldbase/client`,
the WebSocket snapshot protocol, and `@meldbase/react`.

Start the explicitly insecure local development server from the repository root:

```sh
go run ./cmd/meld serve --db /tmp/meldbase-todos.meld --addr :8080 --dev-no-auth
```

Then start the example in another terminal:

```sh
pnpm --filter @meldbase/example-realtime-todos dev
```

Open `http://127.0.0.1:5173`. Use two browser windows to see the full snapshot
subscription update both views. Set `VITE_MELDBASE_URL` when the API is not at
`http://localhost:8080`.

`--dev-no-auth` grants full access and must never be used for a public server.

## Workspace-authenticated server

For a server configured with JWT workspace isolation, provide an access token
issued for the active workspace. The example forwards it as a Bearer token to
both HTTP and the realtime-ticket request; it never sends a client-selected
workspace or tenant value:

```sh
VITE_MELDBASE_URL=https://api.example.test \
VITE_MELDBASE_TOKEN='eyJ...' \
pnpm --filter @meldbase/example-realtime-todos dev
```

The token must satisfy the server's issuer, audience, expiry and
`workspace_id` claim configuration. To switch workspaces, obtain a new token
from the identity system and restart the example with that token. Production
applications should integrate that refresh/switch flow with their own identity
provider rather than storing an arbitrary workspace selector in the browser.
