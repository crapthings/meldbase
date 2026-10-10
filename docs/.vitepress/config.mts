import { defineConfig } from "vitepress";

/** Detects CI builds that need the GitHub Pages URL prefix. */
const isGitHubPagesBuild = process.env.GITHUB_ACTIONS === "true";

/** Configures documentation branding, navigation, and deployment paths. */
export default defineConfig({
  title: "Meldbase",
  description: "A single-file Go document database with queries and live updates.",
  base: isGitHubPagesBuild ? "/meldbase/" : "/",
  cleanUrls: true,
  themeConfig: {
    logo: "/mark.svg",
    search: { provider: "local" },
    outline: { level: [2, 3] },
    nav: [
      {
        text: "Get started",
        items: [
          { text: "Go tour", link: "/getting-started" },
          { text: "Browser", link: "/browser" },
          { text: "React starter", link: "/react-starter" },
          { text: "Node.js", link: "/node" },
        ],
      },
      { text: "Documentation", link: "/overview" },
      { text: "API reference", link: "/api/typescript/", target: "_self" },
    ],
    sidebar: [
      {
        text: "Start",
        items: [
          { text: "Overview", link: "/overview" },
          { text: "Installation", link: "/installation" },
          { text: "Go quickstart", link: "/getting-started" },
          { text: "Browser quickstart", link: "/browser" },
          { text: "React starter", link: "/react-starter" },
          { text: "Node.js quickstart", link: "/node" },
        ],
      },
      {
        text: "Use Meldbase",
        items: [
          { text: "Documents and types", link: "/documents" },
          { text: "Queries and pagination", link: "/queries" },
          { text: "Indexes", link: "/indexes" },
          { text: "Live queries", link: "/realtime" },
          { text: "SDK compatibility", link: "/sdk-status" },
          { text: "Beta checklist", link: "/sdk-beta-checklist" },
          { text: "Publishing SDKs", link: "/sdk-beta-release" },
        ],
      },
      { text: "Reference", items: [{ text: "TypeScript API", link: "/api/typescript/", target: "_self" }] },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/crapthings/meldbase" }],
    footer: {
      message: "Released under the Apache License 2.0.",
      copyright: "Copyright © 2026 Meldbase contributors",
    },
  },
});
