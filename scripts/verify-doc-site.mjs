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
console.log(`Verified API entry and ${links} links bypass the documentation router.`);
