import { readFileSync, writeFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";

/** Runs the published Go quickstart twice to verify its durable-file behavior. */
const root = resolve(import.meta.dirname, "..");
const source = readFileSync(join(root, "docs/getting-started.md"), "utf8").match(/```go\n([\s\S]*?)```/)?.[1];
assert(source, "Go quickstart must contain a complete program");
const directory = mkdtempSync(join(tmpdir(), "meldbase-docs-"));
try {
  writeFileSync(join(directory, "main.go"), source);
  writeFileSync(
    join(directory, "go.mod"),
    `module example.com/meldbase-tour\n\ngo 1.23\n\nrequire github.com/crapthings/meldbase v0.0.0\nreplace github.com/crapthings/meldbase => ${root}\n`,
  );
  for (const count of [1, 2]) {
    const output = execFileSync("go", ["run", "-mod=mod", "."], { cwd: directory, encoding: "utf8" });
    assert.equal(output.trim(), `open todos: ${count}`);
  }
  console.log("Documentation Go quickstart: write, read, and reopen passed.");
} finally {
  rmSync(directory, { recursive: true, force: true });
}
