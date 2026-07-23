# @meldbase/react

Thin React adapter for local and remote Meldbase live queries. It uses
`useSyncExternalStore`, supports server snapshots for hydration, and does not
introduce another cache or query language. It is a beta adapter for queries
created by `LocalCollection.find()` or `RemoteCollection.find()`.

```tsx
import { useLiveQuery } from "@meldbase/react";

export function TodoList({ query }) {
  const { documents, status, error } = useLiveQuery(query, {
    initialData: [],
  });
  if (error) return <p>{error.message}</p>;
  return (
    <ul data-status={status}>
      {documents.map((todo) => (
        <li key={todo._id}>{String(todo.title)}</li>
      ))}
    </ul>
  );
}
```

`@meldbase/client` and React 18 or newer are peer dependencies.

For a remote query, provide the same `initialData` during SSR and the first
client render. The hook exposes `status` (`idle`, `connecting`, `live`,
`stale`, `resyncing`, `error`, or `closed`) and `error`; render those states
instead of treating the initial snapshot as proof that a subscription is live.
The hook owns only the subscription lifecycle. It does not mirror props into a
cache, retry writes, or introduce a second query syntax.

```tsx
const query = client.collection<Todo>("todos").find({ done: false });
const result = useLiveQuery(query, { initialData: serverTodos });

if (result.status === "error") return <RetryView error={result.error} />;
return <TodoList todos={result.documents} stale={result.status === "stale"} />;
```

See the [SDK guide](https://crapthings.github.io/meldbase/sdk#react-hydration-and-live-state)
for hydration and reconnect behavior.
