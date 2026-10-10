import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Locates the repository containing generated SDK documentation.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Selects the public TypeDoc output directory.
const output = join(root, "docs/public/api/typescript");
// Collects generated documentation files for public API checks.
const files = walk(output);

assert(files.length > 0, "TypeDoc did not emit SDK API documentation");
assert(
  !files.some((file) => file.includes(".internal.")),
  "implementation-only @meldbase/client/internal APIs leaked into public TypeDoc output",
);

console.log("verified TypeDoc excludes implementation-only SDK APIs");

// Lists regular files recursively beneath a directory.
function walk(directory) {
  const paths = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) paths.push(...walk(path));
    else if (entry.isFile()) paths.push(path);
  }
  return paths;
}
