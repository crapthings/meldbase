import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const staging = mkdtempSync(join(tmpdir(), "meldbase-sdk-beta-"));
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
    execFileSync(
      "npm",
      ["publish", join(destination, archives[0]), "--access", "public", "--tag", "beta", "--provenance"],
      {
        cwd: root,
        stdio: "inherit",
      },
    );
  }
} finally {
  rmSync(staging, { recursive: true, force: true });
}

function assertCleanGit() {
  const status = execFileSync("git", ["status", "--porcelain"], { cwd: root, encoding: "utf8" }).trim();
  assert.equal(status, "", "SDK publishing requires a clean Git worktree");
}
