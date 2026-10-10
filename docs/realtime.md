---
next: false
---

# Live queries

A live query gives your application updated query results as data changes.
Browser and Node clients connect to the Meldbase server over WebSocket.
The homepage uses the local, in-memory SDK to demonstrate the same interaction.

## Subscribe to a result

After creating a client using the [Browser](/browser) or [Node.js](/node) guide:

```js
const stop = todos.find({ done: false }).subscribe(
  (documents) => {
    console.log("Open todos:", documents);
  },
  {
    onStatus(status) {
      console.log("Connection:", status.state);
    },
  },
);
```

Keep `stop` and call it when the owning view or job ends. Call `client.close()`
when the application no longer needs the client.

## Writes update the view

```js
const todo = await todos.insertOne({ title: "Watch this appear", done: false });
await todos.updateOne({ _id: todo._id }, { $set: { done: true } });
```

The document enters the open-todo result on insertion and leaves it when done
becomes true. Render each delivered snapshot rather than assuming a write's
local return value is the full query result.

## Reconnect deliberately

The remote client reconnects subscriptions after a temporary connection loss.
Use status updates to display stale or resynchronizing data. A lost connection
during a write can leave its outcome uncertain; reconcile the result before
blindly retrying the mutation.

## Run a two-window example

The [Browser quickstart](/browser) runs the public React todo example. Open it
in two windows, add a todo in one, and observe the other. The example also
shows React subscription cleanup and connection state.

The local SDK demonstration does not test network recovery, server persistence,
or authentication. Use the server-backed example for that integration path.
