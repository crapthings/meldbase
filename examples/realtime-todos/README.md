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
