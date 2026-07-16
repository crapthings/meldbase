# @meldbase/react

Thin React adapter for local and remote Meldbase live queries. It uses
`useSyncExternalStore`, supports server snapshots for hydration, and does not
introduce another cache or query language. The package is an alpha preview.

```tsx
import { useLiveQuery } from "@meldbase/react";

export function TodoList({ query }) {
  const { documents, status, error } = useLiveQuery(query, {
    initialData: [],
  });
  if (error) return <p>{error.message}</p>;
  return <ul data-status={status}>{documents.map((todo) =>
    <li key={todo._id}>{String(todo.title)}</li>
  )}</ul>;
}
```

`@meldbase/client` and React 18 or newer are peer dependencies.
