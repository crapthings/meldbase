import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Locates the repository whose SDK release metadata is checked.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Lists the SDK package directories and expected published names.
const packages = [
  { directory: "sdk/client", name: "@meldbase/client" },
  { directory: "sdk/worker", name: "@meldbase/worker" },
  { directory: "sdk/react", name: "@meldbase/react" },
];
// Lists documentation required by the SDK release checklist.
const requiredDocumentation = [
  "docs/browser.md",
  "docs/node.md",
  "sdk/client/README.md",
  "sdk/worker/README.md",
  "sdk/react/README.md",
  "docs/sdk-beta-release.md",
  "docs/sdk-beta-checklist.md",
  "docs/sdk-status.md",
  "sdk/api/README.md",
];

// Loads package manifests alongside their expected names and locations.
const manifests = packages.map((definition) => ({
  ...definition,
  manifest: readJSON(join(definition.directory, "package.json")),
}));
// Selects the client package version as the shared SDK release version.
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
// Loads the checklist that must state the current SDK version.
const checklist = readText("docs/sdk-beta-checklist.md");
assert(
  checklist.includes(`Workspace SDK version: \`${version}\``),
  "docs/sdk-beta-checklist.md must state the current SDK package version",
);
// Loads documentation navigation to check the current SDK guide links.
const docsConfig = readText("docs/.vitepress/config.mts");
assert(
  docsConfig.includes('link: "/browser"') && docsConfig.includes('link: "/node"'),
  "Browser and Node.js SDK guides must be linked from the documentation navigation",
);

console.log(`SDK release checklist is current for ${version}`);

// Parses a repository file as JSON.
function readJSON(path) {
  return JSON.parse(readText(path));
}

// Reads a repository-relative file as UTF-8 text.
function readText(path) {
  return readFileSync(join(root, path), "utf8");
}
