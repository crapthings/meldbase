import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const temporary = mkdtempSync(join(tmpdir(), "meldbase-sdk-pack-"));

const packages = [
  {
    directory: "sdk/client",
    name: "@meldbase/client",
    expected: [
      "LICENSE", "README.md", "package.json",
      ...["index", "local", "mutation", "protocol", "query", "remote", "safe-value", "types", "wire"]
        .flatMap((name) => [`dist/${name}.d.ts`, `dist/${name}.js`]),
    ],
  },
  {
    directory: "sdk/server",
    name: "@meldbase/server",
    expected: [
      "LICENSE", "README.md", "package.json",
      ...["definitions", "errors", "index", "protocol", "shared", "transaction", "types", "worker"]
        .flatMap((name) => [`dist/${name}.d.ts`, `dist/${name}.js`]),
    ],
  },
  {
    directory: "sdk/react",
    name: "@meldbase/react",
    expected: ["LICENSE", "README.md", "package.json", "dist/index.d.ts", "dist/index.js"],
  },
];

try {
  for (const definition of packages) {
    definition.packDirectory = join(temporary, definition.name.replace("@meldbase/", ""));
    mkdirSync(definition.packDirectory, { recursive: true });
    execFileSync("pnpm", ["pack", "--pack-destination", definition.packDirectory], {
      cwd: join(root, definition.directory),
      stdio: "pipe",
    });
    const archives = readdirSync(definition.packDirectory).filter((name) => name.endsWith(".tgz"));
    assert.equal(archives.length, 1, `${definition.name}: expected exactly one tarball`);
    definition.archive = join(definition.packDirectory, archives[0]);
    definition.extractDirectory = join(definition.packDirectory, "extract");
    mkdirSync(definition.extractDirectory);
    execFileSync("tar", ["-xzf", definition.archive, "-C", definition.extractDirectory]);
    definition.packageDirectory = join(definition.extractDirectory, "package");

    const actual = execFileSync("tar", ["-tzf", definition.archive], { encoding: "utf8" })
      .trim().split("\n")
      .map((entry) => entry.replace(/^package\//, "").replace(/\/$/, ""))
      .filter(Boolean)
      .sort();
    assert.deepEqual(actual, [...definition.expected].sort(), `${definition.name}: unexpected published files`);
    assert(!actual.some((name) => /(?:^|\/)\.?[^/]*(?:test|spec)\.[^/]+$/i.test(name)), `${definition.name}: test artifact published`);
    assert(!actual.some((name) => /\.(?:map|meld2?|wal|db|sqlite)$/i.test(name)), `${definition.name}: forbidden artifact published`);

    definition.manifest = JSON.parse(readFileSync(join(definition.packageDirectory, "package.json"), "utf8"));
    assert.equal(definition.manifest.name, definition.name);
    assert.equal(definition.manifest.sideEffects, false, `${definition.name}: sideEffects must be false`);
    assert(!JSON.stringify(definition.manifest).includes("workspace:"), `${definition.name}: workspace range leaked into tarball`);
    verifyExports(definition);
    verifyImports(definition);
  }

  const version = packages[0].manifest.version;
  assert.match(version, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/, "invalid SDK version");
  for (const definition of packages) assert.equal(definition.manifest.version, version, "SDK package versions must move together");
  assert.equal(packages[1].manifest.dependencies["@meldbase/client"], version, "server client dependency must be the packed version");
  assert.equal(packages[2].manifest.peerDependencies["@meldbase/client"], version, "React client peer must be the packed version");

  verifyRuntimeConsumer();
  verifyTypeScriptConsumer();
  console.log(`verified ${packages.length} SDK tarballs at version ${version}`);
} finally {
  rmSync(temporary, { recursive: true, force: true });
}

function verifyExports(definition) {
  assert(definition.manifest.exports && typeof definition.manifest.exports === "object", `${definition.name}: exports required`);
  for (const [subpath, target] of Object.entries(definition.manifest.exports)) {
    assert(target && typeof target === "object", `${definition.name} ${subpath}: conditional export required`);
    for (const condition of ["types", "default"]) {
      const path = target[condition];
      assert.equal(typeof path, "string", `${definition.name} ${subpath}: ${condition} export required`);
      assert(path.startsWith("./dist/"), `${definition.name} ${subpath}: export must stay in dist`);
      assert(existsSync(join(definition.packageDirectory, path)), `${definition.name} ${subpath}: missing ${path}`);
    }
  }
}

function verifyImports(definition) {
  const declared = new Set([
    ...Object.keys(definition.manifest.dependencies ?? {}),
    ...Object.keys(definition.manifest.peerDependencies ?? {}),
  ]);
  for (const path of walk(definition.packageDirectory).filter((name) => /\.d?\.[cm]?ts$|\.[cm]?js$/.test(name))) {
    const source = readFileSync(path, "utf8");
    const specifiers = [...source.matchAll(/(?:\bfrom\s*|\bimport\s*\(|\bexport\s+[^;]*?\bfrom\s*)["']([^"']+)["']/g)]
      .map((match) => match[1]);
    for (const specifier of specifiers) {
      if (specifier.startsWith(".")) {
        assert(existsSync(resolve(dirname(path), specifier)), `${definition.name}: ${relative(definition.packageDirectory, path)} imports missing ${specifier}`);
      } else if (!specifier.startsWith("node:")) {
        const dependency = specifier.startsWith("@") ? specifier.split("/").slice(0, 2).join("/") : specifier.split("/")[0];
        assert(declared.has(dependency), `${definition.name}: undeclared import ${specifier}`);
      }
    }
  }
}

function verifyRuntimeConsumer() {
  const consumer = join(temporary, "runtime-consumer");
  const modules = join(consumer, "node_modules");
  mkdirSync(join(modules, "@meldbase"), { recursive: true });
  for (const definition of packages) {
    cpSync(definition.packageDirectory, join(modules, definition.name), { recursive: true });
  }
  symlinkSync(realPackage("sdk/react/node_modules/react"), join(modules, "react"), "junction");
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
  writeFileSync(join(consumer, "smoke.mjs"), `
    import assert from "node:assert/strict";
    import { LocalCollection, MeldbaseClient, MELDBASE_PROTOCOL_VERSION } from "@meldbase/client";
    import { LocalCollection as LocalSubpath } from "@meldbase/client/local";
    import { MeldbaseClient as RemoteSubpath } from "@meldbase/client/remote";
    import { DEFAULT_QUERY_LIMITS } from "@meldbase/client/types";
    import { MeldbaseWorker, rpc } from "@meldbase/server";
    import { useLiveQuery } from "@meldbase/react";
    assert.equal(LocalCollection, LocalSubpath);
    assert.equal(MeldbaseClient, RemoteSubpath);
    assert.equal(MELDBASE_PROTOCOL_VERSION, 1);
    assert(DEFAULT_QUERY_LIMITS.maxLimit > 0);
    assert.equal(typeof MeldbaseWorker, "function");
    assert.equal(typeof rpc, "function");
    assert.equal(typeof useLiveQuery, "function");
  `);
  execFileSync(process.execPath, [join(consumer, "smoke.mjs")], { cwd: consumer, stdio: "pipe" });
}

function verifyTypeScriptConsumer() {
  const consumer = join(temporary, "typescript-consumer");
  const modules = join(consumer, "node_modules");
  mkdirSync(join(modules, "@meldbase"), { recursive: true });
  mkdirSync(join(modules, "@types"), { recursive: true });
  for (const definition of packages) {
    cpSync(definition.packageDirectory, join(modules, definition.name), { recursive: true });
  }
  symlinkSync(realPackage("sdk/react/node_modules/react"), join(modules, "react"), "junction");
  symlinkSync(realPackage("sdk/react/node_modules/@types/react"), join(modules, "@types/react"), "junction");
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
  writeFileSync(join(consumer, "tsconfig.json"), JSON.stringify({
    compilerOptions: { target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true, noEmit: true, skipLibCheck: true },
    files: ["smoke.ts"],
  }));
  writeFileSync(join(consumer, "smoke.ts"), `
    import { LocalCollection, type Document } from "@meldbase/client";
    import type { RemoteLiveQuery } from "@meldbase/client/remote";
    import { rpc, type MethodDefinition } from "@meldbase/server";
    import { useLiveQuery, type LiveQueryResult } from "@meldbase/react";
    const collection = new LocalCollection<Document>();
    const result: LiveQueryResult<Document> = useLiveQuery(collection.find());
    const method: MethodDefinition = rpc((_context, values) => values[0] ?? null);
    declare const remote: RemoteLiveQuery<Document>;
    useLiveQuery(remote);
    void result; void method;
  `);
  const tsc = realPackage("sdk/client/node_modules/typescript/bin/tsc");
  execFileSync(process.execPath, [tsc, "-p", join(consumer, "tsconfig.json")], { cwd: consumer, stdio: "pipe" });
}

function realPackage(path) {
  return resolve(root, path);
}

function walk(directory) {
  const paths = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) paths.push(...walk(path));
    else if (entry.isFile()) paths.push(path);
  }
  return paths;
}
