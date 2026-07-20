import { defineConfig } from "vitepress";

const isGitHubPagesBuild = process.env.GITHUB_ACTIONS === "true";

export default defineConfig({
  title: "Meldbase",
  description: "A local document database that keeps application data live.",
  base: isGitHubPagesBuild ? "/meldbase/" : "/",
  cleanUrls: true,
  themeConfig: {
    logo: "/mark.svg",
    nav: [
      { text: "Guide", link: "/guide/getting-started" },
      { text: "Concepts", link: "/architecture" },
      { text: "Reference", link: "/reference/" },
      { text: "TypeScript API", link: "/api/typescript/" },
      { text: "Operations", link: "/operations/" },
      { text: "GitHub", link: "https://github.com/crapthings/meldbase" },
    ],
    sidebar: {
      "/guide/": [
        {
          text: "Guide",
          items: [
            { text: "Getting started", link: "/guide/getting-started" },
            { text: "Build a live todo app", link: "/guide/realtime-todos" },
            { text: "Identity and workspaces", link: "/guide/identity-and-workspaces" },
            { text: "Collection access policies", link: "/guide/access-policies" },
          ],
        },
      ],
      "/reference/": [
        {
          text: "Reference",
          items: [
            { text: "Overview", link: "/reference/" },
            { text: "TypeScript API", link: "/api/typescript/" },
            { text: "CLI and configuration", link: "/reference/cli" },
            { text: "HTTP and realtime", link: "/reference/http" },
          ],
        },
      ],
      "/operations/": [
        {
          text: "Operations",
          items: [
            { text: "Overview", link: "/operations/" },
            { text: "Single-node deployment", link: "/single-node-deployment" },
            { text: "Backup and upgrade runbook", link: "/operations/backup-and-upgrade" },
            { text: "Observability", link: "/observability" },
            { text: "Filesystem qualification", link: "/filesystem-qualification" },
            { text: "Release process", link: "/releasing" },
          ],
        },
      ],
      "/": [
        {
          text: "Start here",
          items: [
            { text: "Getting started", link: "/guide/getting-started" },
            { text: "Evaluate safely", link: "/alpha-evaluation" },
            { text: "Current capability audit", link: "/mvp-audit" },
            { text: "Roadmap", link: "/roadmap" },
          ],
        },
        {
          text: "Concepts",
          items: [
            { text: "Architecture", link: "/architecture" },
            { text: "Storage format", link: "/storage-format" },
            { text: "Query contract", link: "/query" },
            { text: "Reactive queries", link: "/reactive" },
            { text: "Client protocol", link: "/client-protocol" },
          ],
        },
        {
          text: "Advanced",
          items: [
            { text: "RPC idempotency", link: "/rpc-idempotency" },
            { text: "Server worker SDK", link: "/server-js-sdk" },
            { text: "Replication protocol", link: "/replication-protocol" },
            { text: "Primary write fence", link: "/primary-lease" },
          ],
        },
      ],
    },
    search: { provider: "local" },
    socialLinks: [{ icon: "github", link: "https://github.com/crapthings/meldbase" }],
    editLink: {
      pattern: "https://github.com/crapthings/meldbase/edit/main/docs/:path",
      text: "Edit this page on GitHub",
    },
    footer: {
      message: "Released under the Apache License 2.0.",
      copyright: "Copyright © 2026 Meldbase contributors",
    },
  },
});
