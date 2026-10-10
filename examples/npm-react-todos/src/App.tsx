import { useState, type FormEvent } from "react";
import { MeldbaseClient } from "@meldbase/client";
import type { Document } from "@meldbase/client/types";
import { useLiveQuery } from "@meldbase/react";

interface Todo extends Document {
  readonly title: string;
  readonly done: boolean;
}

// Keep identity refresh on the server; no signing secret enters the browser.
const client = new MeldbaseClient({
  baseUrl: new URL("/api", window.location.origin).href,
  async accessToken() {
    const response = await fetch("/session", { cache: "no-store" });
    if (!response.ok) throw new Error("Session unavailable. Restart the local starter.");
    const session = (await response.json()) as { accessToken: string };
    return session.accessToken;
  },
});
const todos = client.collection<Todo>("todos");
const query = todos.find({}, { sort: [{ path: "_id", direction: 1 }] });
window.addEventListener("pagehide", () => client.close());

export function App() {
  const { documents, status, error } = useLiveQuery(query);
  const [title, setTitle] = useState("");
  const [pending, setPending] = useState(false);
  const [mutationError, setMutationError] = useState<string>();

  async function mutate(action: () => Promise<unknown>) {
    if (pending) return;
    setPending(true);
    setMutationError(undefined);
    try {
      await action();
    } catch (caught) {
      setMutationError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setPending(false);
    }
  }

  function add(event: FormEvent) {
    event.preventDefault();
    const value = title.trim();
    if (!value) return;
    void mutate(async () => {
      await todos.insertOne({ title: value, done: false });
      setTitle("");
    });
  }

  return (
    <main>
      <p className="eyebrow">Meldbase · React starter</p>
      <h1>
        A shared list.
        <br />
        Always in sync.
      </h1>
      <p className="intro">Add a task, then open a second window. Both views follow the same live query.</p>
      <div id="status" role="status" data-state={status}>
        {status === "live"
          ? "Live"
          : status === "stale"
            ? "Offline · showing saved snapshot"
            : status === "error"
              ? "Connection error"
              : "Connecting…"}
      </div>
      <section aria-label="Tasks">
        <form onSubmit={add}>
          <label htmlFor="title" className="sr-only">
            Task title
          </label>
          <input
            id="title"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            placeholder="What needs doing?"
            maxLength={240}
          />
          <button disabled={pending || !title.trim()}>Add task</button>
        </form>
        {mutationError || error ? <p role="alert">{mutationError ?? error?.message}</p> : null}
        <ul id="todos">
          {documents.map((todo) => (
            <li key={todo._id} data-id={todo._id} data-done={todo.done}>
              <label>
                <input
                  type="checkbox"
                  checked={todo.done}
                  disabled={pending}
                  onChange={() => void mutate(() => todos.updateOne({ _id: todo._id }, { $set: { done: !todo.done } }))}
                />
                <span className={todo.done ? "done" : ""}>{todo.title}</span>
              </label>
              <button
                className="delete"
                aria-label={`Delete ${todo.title}`}
                disabled={pending}
                onClick={() => void mutate(() => todos.deleteOne({ _id: todo._id }))}
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
        {documents.length === 0 && status === "live" ? (
          <p className="empty">A fresh start. Add your first task.</p>
        ) : null}
      </section>
      <footer>Local demo workspace · JWT authenticated</footer>
    </main>
  );
}
