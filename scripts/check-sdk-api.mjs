import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import assert from "node:assert/strict";
import ts from "typescript";
import { resolve } from "node:path";
import { Extractor, ExtractorConfig } from "@microsoft/api-extractor";

/** Locates the workspace and the five supported application entry points. */
const root = resolve(import.meta.dirname, "..");
const entries = [
  ["client", "index"],
  ["client", "local"],
  ["client", "types"],
  ["react", "index"],
  ["worker", "index"],
];
const update = process.argv.includes("--update");
const exports = {};
/** Builds declarations before comparing their public API with the reviewed reports. */
execFileSync("pnpm", ["run", "build:docs-sdk"], { cwd: root, stdio: "inherit" });
let passed = true;
for (const [name, entry] of entries) {
  const configuration = ExtractorConfig.loadFileAndPrepare(resolve(root, `sdk/${name}/api-extractor.${entry}.json`));
  const entryPath = resolve(root, `sdk/${name}/dist/${entry}.d.ts`);
  const program = ts.createProgram([entryPath], {
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    skipLibCheck: true,
  });
  const checker = program.getTypeChecker();
  const symbol = checker.getSymbolAtLocation(program.getSourceFile(entryPath));
  assert(symbol, `No module symbol for ${entryPath}`);
  exports[`${name}/${entry}`] = checker
    .getExportsOfModule(symbol)
    .map((exported) => {
      const declarations = exported.declarations ?? [];
      const typeOnly =
        declarations.length > 0 &&
        declarations.every(
          (declaration) =>
            ts.isExportSpecifier(declaration) && (declaration.isTypeOnly || declaration.parent.parent.isTypeOnly),
        );
      const target = exported.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exported) : exported;
      return {
        name: exported.name,
        type: Boolean(target.flags & ts.SymbolFlags.Type),
        value: !typeOnly && Boolean(target.flags & ts.SymbolFlags.Value),
      };
    })
    .sort((left, right) => left.name.localeCompare(right.name, "en"));
  const result = Extractor.invoke(configuration, { localBuild: update, showVerboseMessages: false });
  passed = result.succeeded && passed;
}
const exportPath = resolve(root, "sdk/api/exports.json");
if (update) writeFileSync(exportPath, JSON.stringify(exports, null, 2) + "\n");
else {
  try {
    assert.deepEqual(exports, JSON.parse(readFileSync(exportPath, "utf8")));
  } catch {
    console.error("Public SDK export names or runtime/type visibility changed.");
    passed = false;
  }
}
if (!passed) {
  console.error("Public SDK API changed. Review the compatibility impact, then run pnpm api:sdk:update.");
  process.exitCode = 1;
} else {
  console.log(
    update ? "Updated public SDK API reports; review and commit the changes." : "Public SDK API baseline matches.",
  );
}
