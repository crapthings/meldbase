import { useRef, useState, type FormEvent } from "react";
import { MeldbaseClient } from "@meldbase/client";
import type { Document } from "@meldbase/client/types";
import { useLiveQuery } from "@meldbase/react";

interface Todo extends Document {
  readonly title: string;
  readonly completed: boolean;
  readonly createdAt: Date;
}

const baseUrl = import.meta.env.VITE_MELDBASE_URL ?? "http://localhost:8080";
const accessToken = import.meta.env.VITE_MELDBASE_TOKEN;
const client = new MeldbaseClient({ baseUrl, ...(accessToken ? { accessToken } : {}) });
const todos = client.collection<Todo>("todos");
const openTodos = todos.find({ completed: false }, { sort: [{ path: "createdAt", direction: 1 }] });

export function App() {
  const { documents, status, error } = useLiveQuery(openTodos);
  const [title, setTitle] = useState("");
  const [mutationError, setMutationError] = useState<Error>();
  const [pending, setPending] = useState(false);
  const pendingTodoIDs = useRef(new Set<string>());
  const [, setPendingTodoVersion] = useState(0);

  function startTodoMutation(id: string): boolean {
    if (pendingTodoIDs.current.has(id)) return false;
    pendingTodoIDs.current.add(id);
    setPendingTodoVersion((version) => version + 1);
    return true;
  }

  function finishTodoMutation(id: string): void {
    pendingTodoIDs.current.delete(id);
    setPendingTodoVersion((version) => version + 1);
  }

  async function addTodo(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextTitle = title.trim();
    if (!nextTitle || pending) return;
    setPending(true);
    setMutationError(undefined);
    try {
      await todos.insertOne({ title: nextTitle, completed: false, createdAt: new Date() });
      setTitle("");
    } catch (caught) {
      setMutationError(asError(caught));
    } finally {
      setPending(false);
    }
  }

  async function complete(todo: Todo) {
    if (!startTodoMutation(todo._id)) return;
    setMutationError(undefined);
    try {
      await todos.updateOne({ _id: todo._id }, { $set: { completed: true } });
    } catch (caught) {
      setMutationError(asError(caught));
    } finally {
      finishTodoMutation(todo._id);
    }
  }

  async function remove(todo: Todo) {
    if (!startTodoMutation(todo._id)) return;
    setMutationError(undefined);
    try {
      await todos.deleteOne({ _id: todo._id });
    } catch (caught) {
      setMutationError(asError(caught));
    } finally {
      finishTodoMutation(todo._id);
    }
  }

  const visibleError = mutationError ?? error;
  const connected = status === "live";

  return (
    <main className="shell">
      <section className="intro" aria-labelledby="page-title">
        <div className="eyebrow">
          <span className="mark" /> Meldbase example
        </div>
        <h1 id="page-title">
          Work that stays
          <br />
          in motion.
        </h1>
        <p>
          One shared query runs through React, WebSocket, and the Go engine. Open this page twice—the list remains live
          in both windows.
        </p>
        <div className={`connection ${connected ? "is-live" : ""}`} role="status" aria-live="polite">
          <span className="connection-dot" />
          <span>{statusLabel(status)}</span>
        </div>
        {accessToken ? (
          <p className="identity-note">Current workspace is selected by the signed access token.</p>
        ) : null}
      </section>

      <section className="workspace" aria-labelledby="todos-title">
        <header className="workspace-header">
          <div>
            <span className="overline">Today</span>
            <h2 id="todos-title">Open tasks</h2>
          </div>
          <span className="count" aria-label={`${documents.length} open tasks`}>
            {documents.length}
          </span>
        </header>

        <form className="composer" onSubmit={(event) => void addTodo(event)}>
          <label className="sr-only" htmlFor="new-todo">
            New task
          </label>
          <input
            id="new-todo"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            placeholder="What needs to move forward?"
            autoComplete="off"
            maxLength={240}
          />
          <button type="submit" disabled={pending || title.trim().length === 0}>
            {pending ? "Adding…" : "Add task"}
          </button>
        </form>

        {visibleError ? (
          <p className="error" role="alert">
            {visibleError.message}
          </p>
        ) : null}

        <ul className="todo-list" aria-live="polite">
          {documents.map((todo) => (
            <li key={todo._id} aria-busy={pendingTodoIDs.current.has(todo._id)}>
              <button
                className="check"
                onClick={() => void complete(todo)}
                disabled={pendingTodoIDs.current.has(todo._id)}
                aria-label={`${pendingTodoIDs.current.has(todo._id) ? "Completing" : "Complete"} ${todo.title}`}
              >
                <span />
              </button>
              <div className="todo-copy">
                <strong>{todo.title}</strong>
                <time dateTime={todo.createdAt.toISOString()}>{formatTime(todo.createdAt)}</time>
              </div>
              <button
                className="remove"
                onClick={() => void remove(todo)}
                disabled={pendingTodoIDs.current.has(todo._id)}
                aria-label={`${pendingTodoIDs.current.has(todo._id) ? "Deleting" : "Delete"} ${todo.title}`}
              >
                {pendingTodoIDs.current.has(todo._id) ? "Working…" : "Delete"}
              </button>
            </li>
          ))}
        </ul>

        {documents.length === 0 && connected ? (
          <div className="empty">
            <span>0</span>
            <p>Nothing open. Add the next useful thing.</p>
          </div>
        ) : null}

        <footer className="workspace-footer">
          <span>
            {accessToken ? "JWT workspace scope · snapshot transport" : "Snapshot transport · data-only query AST"}
          </span>
          <code>{baseUrl}</code>
        </footer>
      </section>
    </main>
  );
}

function statusLabel(status: string): string {
  switch (status) {
    case "live":
      return "Live connection";
    case "stale":
      return "Offline · showing last snapshot";
    case "resyncing":
      return "Refreshing snapshot";
    case "error":
      return "Connection error";
    case "closed":
      return "Connection closed";
    default:
      return "Connecting";
  }
}

function formatTime(value: Date): string {
  return new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(value);
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
