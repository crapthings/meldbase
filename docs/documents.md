# Documents and collections

A database contains named collections. Documents contain typed fields and a
canonical `_id`: a nonzero, 32-character lowercase hexadecimal string.
The SDK generates an ID when you omit it during insertion.

## Write a todo

The following TypeScript excerpts assume `todos` is a collection from the
[Browser](/browser) or [Node.js](/node) quickstart.

```js
const todo = await todos.insertOne({ title: "Read the docs", done: false });
const open = await todos.find({ done: false }).fetch();
console.log(open);
```

## Update and delete

```js
await todos.updateOne({ _id: todo._id }, { $set: { done: true } });
await todos.deleteOne({ _id: todo._id });
```

Use the ID returned by the insert when targeting a particular document.
Queries and updates are data objects rather than executable JavaScript.

## Typed values

| Value                     | JavaScript representation               |
| ------------------------- | --------------------------------------- |
| Null, boolean, string     | `null`, `true`, `"hello"`               |
| Float64                   | `number`                                |
| Int64                     | `bigint` within the signed 64-bit range |
| Timestamp                 | `Date`                                  |
| Binary                    | `Uint8Array`                            |
| Arrays and nested objects | Arrays and plain document objects       |

The SDK handles the wire encoding for these values. `undefined` is not a stored
value. An absent field differs from a field set to `null`. Prefer `bigint` for
int64 values rather than allowing a JavaScript number to lose precision.

Go code uses constructors such as `meldbase.String` and `meldbase.Bool`; see
the [complete Go tour](/getting-started).

## Next step

[Filter and sort documents](/queries), or [subscribe to changes](/realtime).
