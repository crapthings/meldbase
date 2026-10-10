// Compile-time contract tests for typed collection inserts. This file is part
// of the normal client TypeScript build; `@ts-expect-error` makes a widening
// regression fail the build without adding runtime test behavior.
import type { RemoteCollection } from "./remote/collection.js";
import { LocalCollection } from "./local.js";
import type { Document, InputDocument } from "./types.js";

/** Defines the required fields used to check typed collection inserts. */
type Todo = Document & {
  readonly title: string;
  readonly done: boolean;
};

/** Provides a typed local collection for insert compatibility checks. */
const local = new LocalCollection<Todo>();
/** Checks that a local insert returns a document with the collection schema. */
const localInserted: Todo = local.insertOne({ title: "write release notes", done: false });
/** Provides a typed remote collection for compile-time insert checks. */
declare const remote: RemoteCollection<Todo>;
/** Checks that a remote insert returns a promise of the collection schema. */
const remoteInserted: Promise<Todo> = remote.insertOne({ title: "write release notes", done: false });

// @ts-expect-error A typed collection cannot claim a missing required field is Todo.
local.insertOne({ title: "missing done" });
// @ts-expect-error An arbitrary transport document cannot be inserted as Todo.
remote.insertOne({ unrelated: true });
/** Provides broad document input that cannot prove the required collection fields. */
declare const arbitraryInput: InputDocument;
// @ts-expect-error A broad InputDocument cannot prove it satisfies Todo.
local.insertOne(arbitraryInput);
// @ts-expect-error A broad InputDocument cannot prove it satisfies Todo.
remote.insertOne(arbitraryInput);

void localInserted;
void remoteInserted;
