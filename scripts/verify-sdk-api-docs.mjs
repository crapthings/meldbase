import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const output = join(root, "docs/public/api/typescript");
const files = walk(output);

assert(files.length > 0, "TypeDoc did not emit SDK API documentation");
assert(
  !files.some((file) => file.includes(".internal.")),
  "implementation-only @meldbase/client/internal APIs leaked into public TypeDoc output",
);

console.log("verified TypeDoc excludes implementation-only SDK APIs");

function walk(directory) {
  const paths = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) paths.push(...walk(path));
    else if (entry.isFile()) paths.push(path);
  }
  return paths;
}
