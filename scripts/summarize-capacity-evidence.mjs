import { mkdir, open, readFile, readdir, stat } from "node:fs/promises";
import { basename, dirname, resolve } from "node:path";

// Returns the capacity summary command syntax.
function usage() {
  return "Usage: node scripts/summarize-capacity-evidence.mjs --in-dir EVIDENCE_DIRECTORY --out NEW_MARKDOWN_FILE";
}

// Throws a capacity evidence validation error.
function fail(message) {
  throw new Error(message);
}

// Validates a reported latency and formats it to one decimal place.
function percentile(value) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) fail("invalid latency in query report");
  return value.toFixed(1);
}

// Escapes table separators and flattens newlines in Markdown cell text.
function markdown(value) {
  return String(value).replaceAll("|", "\\|").replaceAll("\n", " ");
}

// Parses the evidence directory and new output file path.
async function parseArgs(args) {
  let input = "";
  let output = "";
  while (args.length > 0) {
    const argument = args.shift();
    if (argument === "--in-dir") input = args.shift() ?? "";
    else if (argument === "--out") output = args.shift() ?? "";
    else if (argument === "--help" || argument === "-h") {
      console.log(usage());
      process.exit(0);
    } else fail(`unknown argument: ${argument}`);
  }
  if (!input || !output) fail(usage());
  return { input: resolve(input), output: resolve(output) };
}

// Reads a JSON evidence file and labels parsing failures with its filename.
async function readJSON(path) {
  let value;
  try {
    value = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    fail(`read ${basename(path)}: ${error.message}`);
  }
  return value;
}

// Writes a private Markdown summary of host facts, query latency, and passing runtime benchmark evidence.
async function main() {
  const { input, output } = await parseArgs(process.argv.slice(2));
  const inputInfo = await stat(input).catch(() => null);
  if (!inputInfo?.isDirectory()) fail("--in-dir must be an existing evidence directory");
  if (await stat(output).catch(() => null)) fail("--out must not already exist");
  const host = await readFile(`${input}/host-capacity.txt`, "utf8").catch((error) => fail(`read host-capacity.txt: ${error.message}`));
  const resourceLimits = await readFile(`${input}/resource-limits.sha256`, "utf8").catch((error) => {
    if (error.code === "ENOENT") return "";
    fail(`read resource-limits.sha256: ${error.message}`);
  });
  const queryDirectory = await readdir(`${input}/query`).catch((error) => fail(`read query evidence: ${error.message}`));
  const queryFiles = queryDirectory.filter((file) => /^query-capacity-[0-9]+\.json$/.test(file)).sort();
  if (queryFiles.length === 0) fail("query evidence contains no query-capacity-*.json report");
  const rows = [];
  for (const file of queryFiles) {
    const report = await readJSON(`${input}/query/${file}`);
    const reports = Array.isArray(report.reports) ? report.reports : [report];
    for (const backendReport of reports) {
      if (backendReport?.schemaVersion !== 1 || !backendReport.config || !Array.isArray(backendReport.scenarios)) fail(`unsupported query report: ${file}`);
      for (const scenario of backendReport.scenarios) {
        if (!scenario?.explain || typeof scenario.name !== "string") fail(`invalid scenario: ${file}`);
        rows.push({
          documents: backendReport.config.documents,
          backend: backendReport.config.backend,
          scenario: scenario.name,
          plan: scenario.explain.Stage || "UNKNOWN",
          p50: percentile(scenario.p50LatencyMicros),
          p95: percentile(scenario.p95LatencyMicros),
          p99: percentile(scenario.p99LatencyMicros),
          keys: scenario.explain.KeysExamined ?? 0,
          documentsExamined: scenario.explain.DocumentsExamined ?? 0,
        });
      }
    }
  }
  const benchmark = await readFile(`${input}/runtime/core-runtime.json`, "utf8").catch((error) => fail(`read runtime benchmark: ${error.message}`));
  const events = benchmark.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
  const passed = events.some((event) => event.Action === "pass" && event.Package === "github.com/crapthings/meldbase/internal/database");
  if (!passed) fail("runtime benchmark has no passing internal/database result");
  await mkdir(dirname(output), { recursive: true, mode: 0o700 });
  const destination = await open(output, "wx", 0o600);
  try {
    const content = [
      "# Meldbase capacity evidence summary",
      "",
      `Evidence directory: \`${input}\``,
      "",
      "This is a machine- and workload-specific observation, not an SLA or capacity promise.",
      "",
      "## Host snapshot",
      "",
      "```text",
      host.trim(),
      "```",
      "",
      "## Resource-limit configuration",
      "",
      resourceLimits ? ["```text", resourceLimits.trim(), "```"].join("\n") : "No resource-limit configuration digest was captured. Compare this run only after confirming the same effective limits manually.",
      "",
      "## Query latency and work",
      "",
      "| Documents | Backend | Scenario | Plan | P50 µs | P95 µs | P99 µs | Keys | Documents examined |",
      "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
      ...rows.map((row) => `| ${row.documents} | ${markdown(row.backend)} | ${markdown(row.scenario)} | ${markdown(row.plan)} | ${row.p50} | ${row.p95} | ${row.p99} | ${row.keys} | ${row.documentsExamined} |`),
      "",
      "## Runtime benchmark",
      "",
      "`runtime/core-runtime.json` contains a passing core durable/database benchmark run. Preserve it with this summary; benchmark JSON is the authoritative allocation and throughput evidence.",
      "",
      "Compare only runs with a reviewed workload, equivalent storage class, Go version, resource-limit configuration, and source revision.",
      "",
    ].join("\n");
    await destination.writeFile(content);
  } finally {
    await destination.close();
  }
}

main().catch((error) => {
  console.error(`summarize-capacity-evidence: ${error.message}`);
  process.exitCode = 1;
});
