import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Locates the repository used for SDK release checks and packaging.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// Creates a temporary directory for the SDK archives to publish.
const artifactDirectory = process.env.MELDBASE_SDK_PACK_DIR;
const staging = artifactDirectory ? resolve(artifactDirectory) : mkdtempSync(join(tmpdir(), "meldbase-sdk-beta-"));
mkdirSync(staging, { recursive: true });
// Lists SDK packages published together under the beta tag.
const packages = [
  { directory: "sdk/client", name: "@meldbase/client" },
  { directory: "sdk/worker", name: "@meldbase/worker" },
  { directory: "sdk/react", name: "@meldbase/react" },
];

try {
  assertCleanGit();
  execFileSync("pnpm", ["run", "release:sdk:checklist"], { cwd: root, stdio: "inherit" });
  const manifests = packages.map((definition) => ({
    ...definition,
    manifest: JSON.parse(readFileSync(join(root, definition.directory, "package.json"), "utf8")),
  }));
  const version = manifests[0].manifest.version;
  assert.match(version, /^\d+\.\d+\.\d+-beta\.\d+$/, "SDK beta releases require an x.y.z-beta.N version");
  for (const definition of manifests) {
    assert.equal(definition.manifest.name, definition.name, `unexpected package name in ${definition.directory}`);
    assert.equal(definition.manifest.version, version, "SDK package versions must move together");
  }

  execFileSync("pnpm", ["run", "pack:check"], { cwd: root, stdio: "inherit" });
  for (const definition of manifests) {
    const destination = join(staging, definition.name.replace("@meldbase/", ""));
    mkdirSync(destination, { recursive: true });
    execFileSync("pnpm", ["pack", "--pack-destination", destination], {
      cwd: join(root, definition.directory),
      stdio: "inherit",
    });
    const archives = readdirSync(destination).filter((name) => name.endsWith(".tgz"));
    assert.equal(archives.length, 1, `${definition.name}: expected exactly one tarball`);
    definition.archive = join(destination, archives[0]);
  }

  // Binds all verified archives to the source commit before any registry write.
  const receipt = {
    version,
    commit: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
    packages: manifests.map((definition) => ({
      name: definition.name,
      archive: definition.archive.slice(staging.length + 1),
      integrity: `sha512-${createHash("sha512").update(readFileSync(definition.archive)).digest("base64")}`,
    })),
  };
  writeFileSync(join(staging, "receipt.json"), JSON.stringify(receipt, null, 2) + "\n");
  for (const definition of manifests) {
    if (process.argv.includes("--pack-only")) continue;
    execFileSync("npm", ["publish", definition.archive, "--access", "public", "--tag", "beta", "--provenance"], {
      cwd: root,
      stdio: "inherit",
    });
  }
  console.log(
    process.argv.includes("--pack-only")
      ? "Verified beta archives prepared without registry publication."
      : `Published SDK beta ${version}.`,
  );
} finally {
  if (!artifactDirectory) rmSync(staging, { recursive: true, force: true });
}

// Rejects SDK publishing when the repository worktree has changes.
function assertCleanGit() {
  const status = execFileSync("git", ["status", "--porcelain"], { cwd: root, encoding: "utf8" }).trim();
  assert.equal(status, "", "SDK publishing requires a clean Git worktree");
}
