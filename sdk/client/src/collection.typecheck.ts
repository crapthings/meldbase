// Compile-time contract tests for typed collection inserts. This file is part
// of the normal client TypeScript build; `@ts-expect-error` makes a widening
// regression fail the build without adding runtime test behavior.
import type { RemoteCollection } from "./remote/collection.js";
import { LocalCollection } from "./local.js";
import type { Document, InputDocument } from "./types.js";

type Todo = Document & {
  readonly title: string;
  readonly done: boolean;
};

const local = new LocalCollection<Todo>();
const localInserted: Todo = local.insertOne({ title: "write release notes", done: false });
declare const remote: RemoteCollection<Todo>;
const remoteInserted: Promise<Todo> = remote.insertOne({ title: "write release notes", done: false });

// @ts-expect-error A typed collection cannot claim a missing required field is Todo.
local.insertOne({ title: "missing done" });
// @ts-expect-error An arbitrary transport document cannot be inserted as Todo.
remote.insertOne({ unrelated: true });
declare const arbitraryInput: InputDocument;
// @ts-expect-error A broad InputDocument cannot prove it satisfies Todo.
local.insertOne(arbitraryInput);
// @ts-expect-error A broad InputDocument cannot prove it satisfies Todo.
remote.insertOne(arbitraryInput);

void localInserted;
void remoteInserted;
