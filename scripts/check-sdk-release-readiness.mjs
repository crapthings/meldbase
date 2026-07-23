import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packages = [
  { directory: "sdk/client", name: "@meldbase/client" },
  { directory: "sdk/worker", name: "@meldbase/worker" },
  { directory: "sdk/react", name: "@meldbase/react" },
];
const requiredDocumentation = ["docs/sdk.md", "docs/sdk-beta-release.md", "docs/sdk-beta-checklist.md"];

const manifests = packages.map((definition) => ({
  ...definition,
  manifest: readJSON(join(definition.directory, "package.json")),
}));
const version = manifests[0].manifest.version;

assert.match(
  version,
  /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/,
  "SDK version must be a valid semver prerelease or release",
);
for (const definition of manifests) {
  assert.equal(definition.manifest.name, definition.name, `unexpected package name in ${definition.directory}`);
  assert.equal(definition.manifest.version, version, "SDK package versions must move together");

  const compiler = readJSON(join(definition.directory, "tsconfig.json")).compilerOptions;
  assert.equal(
    compiler.sourceMap,
    false,
    `${definition.name}: source maps must be explicitly disabled for the published SDK`,
  );
  assert.equal(
    compiler.declarationMap,
    false,
    `${definition.name}: declaration maps must be explicitly disabled for the published SDK`,
  );
}

for (const path of requiredDocumentation) {
  const content = readText(path);
  assert(content.trim().length > 0, `${path}: SDK documentation must not be empty`);
}
const checklist = readText("docs/sdk-beta-checklist.md");
assert(
  checklist.includes(`Workspace SDK version: \`${version}\``),
  "docs/sdk-beta-checklist.md must state the current SDK package version",
);
const docsConfig = readText("docs/.vitepress/config.mts");
assert(
  docsConfig.includes('link: "/sdk-beta-checklist"'),
  "SDK beta checklist must be linked from the documentation navigation",
);

console.log(`SDK release checklist is current for ${version}`);

function readJSON(path) {
  return JSON.parse(readText(path));
}

function readText(path) {
  return readFileSync(join(root, path), "utf8");
}
