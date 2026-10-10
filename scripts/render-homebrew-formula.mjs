import { open } from "node:fs/promises";
import { resolve } from "node:path";

// Throws a command validation error.
function fail(message) {
  throw new Error(message);
}

// Returns the formula generator command syntax.
function usage() {
  return "Usage: node scripts/render-homebrew-formula.mjs --version X.Y.Z-alpha.N --arm64-sha256 HEX --amd64-sha256 HEX --out NEW_FORMULA_PATH";
}

// Validates alpha release arguments and resolves the output file path.
function readArguments(args) {
  const values = new Map();
  while (args.length > 0) {
    const key = args.shift();
    if (key === "--help" || key === "-h") {
      console.log(usage());
      process.exit(0);
    }
    if (!["--version", "--arm64-sha256", "--amd64-sha256", "--out"].includes(key)) fail(`unknown argument: ${key}`);
    const value = args.shift();
    if (!value) fail(`missing value for ${key}`);
    values.set(key, value);
  }
  const version = values.get("--version");
  const arm64 = values.get("--arm64-sha256");
  const amd64 = values.get("--amd64-sha256");
  const output = values.get("--out");
  if (!version || !arm64 || !amd64 || !output) fail(usage());
  if (!/^0\.[0-9]+\.[0-9]+-alpha\.[0-9]+$/.test(version)) fail("--version must be an alpha semantic version without a leading v");
  if (!/^[a-f0-9]{64}$/.test(arm64) || !/^[a-f0-9]{64}$/.test(amd64)) fail("archive checksums must be lower-case SHA-256 hex");
  return { version, arm64, amd64, output: resolve(output) };
}

// Creates a new Homebrew formula with release archive URLs and supplied checksums.
async function main() {
  const { version, arm64, amd64, output } = readArguments(process.argv.slice(2));
  const tag = `v${version}`;
  const base = `https://github.com/crapthings/meldbase/releases/download/${tag}`;
  const body = `class Meldbase < Formula
  desc "Embedded document database with realtime query support"
  homepage "https://github.com/crapthings/meldbase"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "${base}/meldbase_${version}_darwin_arm64.tar.gz"
      sha256 "${arm64}"
    end

    on_intel do
      url "${base}/meldbase_${version}_darwin_amd64.tar.gz"
      sha256 "${amd64}"
    end
  end

  def install
    bin.install "meld"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/meld version")
  end
end
`;
  const destination = await open(output, "wx", 0o644);
  try {
    await destination.writeFile(body);
  } finally {
    await destination.close();
  }
}

main().catch((error) => {
  console.error(`render-homebrew-formula: ${error.message}`);
  process.exitCode = 1;
});
