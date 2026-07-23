import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
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
      "LICENSE",
      "README.md",
      "package.json",
      ...[
        "cursor",
        "index",
        "internal",
        "local",
        "mutation",
        "protocol",
        "query",
        "remote",
        "safe-value",
        "types",
        "wire",
      ].flatMap((name) => [`dist/${name}.d.ts`, `dist/${name}.js`]),
      ...[
        "observer",
        "remote/client",
        "remote/collection",
        "remote/errors",
        "remote/realtime",
        "remote/shared",
        "remote/types",
      ].flatMap((name) => [`dist/${name}.d.ts`, `dist/${name}.js`]),
    ],
  },
  {
    directory: "sdk/worker",
    name: "@meldbase/worker",
    expected: [
      "LICENSE",
      "README.md",
      "package.json",
      ...["definitions", "errors", "index", "protocol", "shared", "transaction", "types", "worker"].flatMap((name) => [
        `dist/${name}.d.ts`,
        `dist/${name}.js`,
      ]),
    ],
  },
  {
    directory: "sdk/react",
    name: "@meldbase/react",
    expected: ["LICENSE", "README.md", "package.json", "dist/index.d.ts", "dist/index.js"],
  },
];

try {
  const sourceRoot = prepareCleanWorkspace();
  for (const definition of packages) {
    const sourceDirectory = join(sourceRoot, definition.directory);
    assert(!existsSync(join(sourceDirectory, "dist")), `${definition.name}: clean checkout unexpectedly contains dist`);
    definition.packDirectory = join(temporary, definition.name.replace("@meldbase/", ""));
    mkdirSync(definition.packDirectory, { recursive: true });
    execFileSync("pnpm", ["pack", "--pack-destination", definition.packDirectory], {
      cwd: sourceDirectory,
      stdio: "pipe",
    });
    assertEmittedArtifacts(definition, sourceDirectory);
    const archives = readdirSync(definition.packDirectory).filter((name) => name.endsWith(".tgz"));
    assert.equal(archives.length, 1, `${definition.name}: expected exactly one tarball`);
    definition.archive = join(definition.packDirectory, archives[0]);
    definition.extractDirectory = join(definition.packDirectory, "extract");
    mkdirSync(definition.extractDirectory);
    execFileSync("tar", ["-xzf", definition.archive, "-C", definition.extractDirectory]);
    definition.packageDirectory = join(definition.extractDirectory, "package");

    const actual = execFileSync("tar", ["-tzf", definition.archive], { encoding: "utf8" })
      .trim()
      .split("\n")
      .map((entry) => entry.replace(/^package\//, "").replace(/\/$/, ""))
      .filter(Boolean)
      .sort();
    assert.deepEqual(actual, [...definition.expected].sort(), `${definition.name}: unexpected published files`);
    assert(
      !actual.some((name) => /(?:^|\/)\.?[^/]*(?:test|spec)\.[^/]+$/i.test(name)),
      `${definition.name}: test artifact published`,
    );
    assert(
      !actual.some((name) => /\.(?:map|meld2?|wal|db|sqlite)$/i.test(name)),
      `${definition.name}: forbidden artifact published`,
    );

    definition.manifest = JSON.parse(readFileSync(join(definition.packageDirectory, "package.json"), "utf8"));
    assert.equal(definition.manifest.name, definition.name);
    assert.equal(definition.manifest.sideEffects, false, `${definition.name}: sideEffects must be false`);
    assert(
      !JSON.stringify(definition.manifest).includes("workspace:"),
      `${definition.name}: workspace range leaked into tarball`,
    );
    verifyExports(definition);
    verifyImports(definition);
  }

  const version = packages[0].manifest.version;
  assert.match(version, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/, "invalid SDK version");
  for (const definition of packages)
    assert.equal(definition.manifest.version, version, "SDK package versions must move together");
  assert.equal(
    packages[1].manifest.dependencies["@meldbase/client"],
    version,
    "server client dependency must be the packed version",
  );
  assert.equal(
    packages[2].manifest.peerDependencies["@meldbase/client"],
    version,
    "React client peer must be the packed version",
  );

  verifyRuntimeConsumer();
  verifyTypeScriptConsumer("NodeNext");
  verifyTypeScriptConsumer("Bundler");
  console.log(`verified ${packages.length} SDK tarballs from a fresh source tree at version ${version}`);
} finally {
  rmSync(temporary, { recursive: true, force: true });
}

function prepareCleanWorkspace() {
  const clean = join(temporary, "clean-checkout");
  mkdirSync(clean, { recursive: true });
  const tracked = execFileSync("git", ["ls-files", "-z"], { cwd: root, encoding: "utf8" }).split("\0").filter(Boolean);
  const untracked = execFileSync("git", ["ls-files", "--others", "--exclude-standard", "-z"], {
    cwd: root,
    encoding: "utf8",
  })
    .split("\0")
    .filter(Boolean);
  const sources = [...new Set([...tracked, ...untracked])];
  assert(sources.length > 0, "repository source is required for clean-checkout packaging verification");
  for (const path of sources) {
    const source = join(root, path);
    const destination = join(clean, path);
    mkdirSync(dirname(destination), { recursive: true });
    cpSync(source, destination, { dereference: false });
  }
  linkInstalledDependencies(clean);
  return clean;
}

function linkInstalledDependencies(clean) {
  const rootModules = join(root, "node_modules");
  assert(existsSync(rootModules), "run pnpm install before SDK package verification");
  symlinkSync(rootModules, join(clean, "node_modules"), "dir");
  for (const definition of packages) {
    const sourceModules = join(root, definition.directory, "node_modules");
    const destinationModules = join(clean, definition.directory, "node_modules");
    assert(existsSync(sourceModules), `run pnpm install before verifying ${definition.name}`);
    cpSync(sourceModules, destinationModules, { recursive: true, dereference: false });
  }
}

function assertEmittedArtifacts(definition, sourceDirectory) {
  const artifacts = definition.expected.filter((path) => path.startsWith("dist/"));
  assert(artifacts.length > 0, `${definition.name}: package must publish build artifacts`);
  for (const artifact of artifacts) {
    const path = join(sourceDirectory, artifact);
    assert(existsSync(path), `${definition.name}: prepack did not emit ${artifact}`);
    assert(readFileSync(path, "utf8").trim().length > 0, `${definition.name}: emitted empty ${artifact}`);
  }
}

function verifyExports(definition) {
  assert(
    definition.manifest.exports && typeof definition.manifest.exports === "object",
    `${definition.name}: exports required`,
  );
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
    const specifiers = [
      ...source.matchAll(/(?:\bfrom\s*|\bimport\s*\(|\bexport\s+[^;]*?\bfrom\s*)["']([^"']+)["']/g),
    ].map((match) => match[1]);
    for (const specifier of specifiers) {
      if (specifier.startsWith(".")) {
        assert(
          existsSync(resolve(dirname(path), specifier)),
          `${definition.name}: ${relative(definition.packageDirectory, path)} imports missing ${specifier}`,
        );
      } else if (!specifier.startsWith("node:")) {
        const dependency = specifier.startsWith("@")
          ? specifier.split("/").slice(0, 2).join("/")
          : specifier.split("/")[0];
        assert(declared.has(dependency), `${definition.name}: undeclared import ${specifier}`);
      }
    }
  }
}

function verifyRuntimeConsumer() {
  const consumer = installTarballConsumer("runtime-consumer");
  writeFileSync(
    join(consumer, "smoke.mjs"),
    `
    import assert from "node:assert/strict";
    import { documentID, isDocumentIDValue, MeldbaseClient } from "@meldbase/client";
    import { LocalCollection as LocalSubpath } from "@meldbase/client/local";
    import { DEFAULT_QUERY_LIMITS } from "@meldbase/client/types";
    import { MeldbaseWorker, rpc } from "@meldbase/worker";
    import { useLiveQuery } from "@meldbase/react";
    assert.equal(typeof LocalSubpath, "function");
    assert(DEFAULT_QUERY_LIMITS.maxLimit > 0);
    assert.equal(typeof MeldbaseWorker, "function");
    assert.equal(typeof rpc, "function");
    assert.equal(typeof useLiveQuery, "function");
    assert.equal(isDocumentIDValue(documentID("00000000000000000000000000000001")), true);
  `,
  );
  execFileSync(process.execPath, [join(consumer, "smoke.mjs")], { cwd: consumer, stdio: "pipe" });
}

function verifyTypeScriptConsumer(resolution) {
  const consumer = installTarballConsumer(`typescript-${resolution.toLowerCase()}-consumer`);
  const modules = join(consumer, "node_modules");
  mkdirSync(join(modules, "@types"), { recursive: true });
  symlinkSync(realPackage("sdk/react/node_modules/@types/react"), join(modules, "@types/react"), "junction");
  writeFileSync(
    join(consumer, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        target: "ES2022",
        module: resolution === "NodeNext" ? "NodeNext" : "ESNext",
        moduleResolution: resolution,
        strict: true,
        noEmit: true,
        skipLibCheck: true,
      },
      files: ["smoke.ts"],
    }),
  );
  writeFileSync(
    join(consumer, "smoke.ts"),
    `
    import { documentID, MeldbaseClient, type Document, type DocumentID, type RemoteLiveQuery } from "@meldbase/client";
    import { LocalCollection } from "@meldbase/client/local";
    import { rpc, type RPCDefinition } from "@meldbase/worker";
    import { useLiveQuery, type LiveQueryResult } from "@meldbase/react";
    type Todo = Document & { readonly title: string; readonly done: boolean };
    const collection = new LocalCollection<Todo>();
    const inserted: Todo = collection.insertOne({ title: "local", done: false });
    const result: LiveQueryResult<Document> = useLiveQuery(collection.find());
    const method: RPCDefinition = rpc((_context, input) => input);
    const owner: DocumentID = documentID("00000000000000000000000000000001");
    declare const remote: RemoteLiveQuery<Document>;
    declare const client: MeldbaseClient;
    const remoteTodos = client.collection<Todo>("todos");
    const remoteInserted: Promise<Todo> = remoteTodos.insertOne({ title: "remote", done: false });
    // @ts-expect-error Typed inserts cannot omit Todo.done.
    remoteTodos.insertOne({ title: "invalid" });
    useLiveQuery(remote);
    void inserted; void result; void method; void owner; void remoteInserted;
  `,
  );
  const tsc = realPackage("sdk/client/node_modules/typescript/bin/tsc");
  execFileSync(process.execPath, [tsc, "-p", join(consumer, "tsconfig.json")], { cwd: consumer, stdio: "pipe" });
}

function installTarballConsumer(name) {
  const consumer = join(temporary, name);
  mkdirSync(consumer, { recursive: true });
  writeFileSync(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
  const localReact = realPackage("sdk/react/node_modules/react");
  const cache = join(temporary, "npm-cache");
  execFileSync(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      "--package-lock=false",
      "--cache",
      cache,
      ...packages.map((definition) => definition.archive),
      localReact,
    ],
    { cwd: consumer, stdio: "pipe" },
  );
  return consumer;
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
