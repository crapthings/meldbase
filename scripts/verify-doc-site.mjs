import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";

/** Locates the built documentation and its independently generated API entry. */
const output = resolve(import.meta.dirname, "../docs/.vitepress/dist");
assert(existsSync(join(output, "api/typescript/index.html")), "Published TypeDoc entry is missing");
let links = 0;
/** Checks that API links bypass the VitePress router in every documentation page. */
for (const name of readdirSync(output)) {
  if (!name.endsWith(".html")) continue;
  const html = readFileSync(join(output, name), "utf8");
  for (const anchor of html.matchAll(/<a\b[^>]*>/g)) {
    if (!/href="[^"]*\/api\/typescript\/(?:index\.html)?"/.test(anchor[0])) continue;
    links++;
    assert(/target="_self"/.test(anchor[0]), `${name}: API reference must use a full-page navigation`);
  }
}
assert(links > 0, "No API reference links found in the published documentation");
const index = readFileSync(join(output, "llms.txt"), "utf8");
assert(index.startsWith("# Meldbase\n"), "LLM index is missing its project heading");
const prefix = "https://crapthings.github.io/meldbase/";
let markdownLinks = 0;
for (const match of index.matchAll(/\]\((https:\/\/[^)]+)\)/g)) {
  if (!match[1].startsWith(prefix)) continue;
  const path = match[1].slice(prefix.length);
  assert(
    existsSync(join(output, path.endsWith("/") ? `${path}index.html` : path)),
    `LLM index target is missing: ${path}`,
  );
  if (path.endsWith(".md")) markdownLinks++;
}
assert(markdownLinks > 0, "LLM index must link to generated Markdown documents");
console.log(`Verified API entry and ${links} links bypass the documentation router.`);
console.log(`Verified llms.txt and ${markdownLinks} Markdown entry points.`);
