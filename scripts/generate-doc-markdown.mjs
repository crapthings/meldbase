import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";

const docs = resolve(import.meta.dirname, "../docs");
const output = join(docs, ".vitepress/dist");
const site = "https://crapthings.github.io/meldbase";
const pages = readdirSync(docs).filter((name) => name.endsWith(".md") && name !== "index.md");
const names = new Set(pages.map((name) => name.slice(0, -3)));

// Publish clean source documents alongside HTML; preserve code examples verbatim.
for (const name of pages) {
  const source = readFileSync(join(docs, name), "utf8");
  const markdown = source
    .replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, "")
    .split(/(^```[^\n]*\n[\s\S]*?^```[^\n]*(?:\n|$))/m)
    .map((part, index) => {
      if (index % 2) return part;
      return part
        .replace(/<script\b[^>]*>[\s\S]*?<\/script>\s*/g, "")
        .replace(
          /<a :href="withBase\('([^']+)'\)"[^>]*>([^<]+)<\/a>/g,
          (_, path, label) => `[${label}](${site}${path})`,
        )
        .replace(/\]\(\/(?!\/)([^)]+)\)/g, (_, path) => {
          const [page, fragment] = path.split("#");
          return `](${site}/${page}${names.has(page) ? ".md" : ""}${fragment ? `#${fragment}` : ""})`;
        });
    })
    .join("")
    .trim();
  writeFileSync(join(output, name), `${markdown}\n`);
}
console.log(`Published ${pages.length} Markdown documents from existing source pages.`);
