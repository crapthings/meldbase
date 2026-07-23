import { hydrateRoot } from "react-dom/client";
import { MeldbaseClient } from "@meldbase/client";
import type { Document } from "@meldbase/client/types";
import { useLiveQuery } from "@meldbase/react";

interface Todo extends Document {
  readonly title: string;
  readonly done: boolean;
}

const initialData: readonly Todo[] = [
  {
    _id: "00000000000000000000000000000001",
    title: "server",
    done: false,
  },
];
const client = new MeldbaseClient({
  baseUrl: import.meta.env.VITE_MELDBASE_URL ?? "http://127.0.0.1:8080",
});
const query = client.collection<Todo>("todos").find({ done: false });

function HydrationProbe() {
  const { documents, status } = useLiveQuery(query, { initialData });
  return (
    <>
      <p id="status">{status}</p>
      <ul id="todos">
        {documents.map((todo) => (
          <li key={todo._id}>{todo.title}</li>
        ))}
      </ul>
    </>
  );
}

hydrateRoot(document.querySelector("#root")!, <HydrationProbe />);
