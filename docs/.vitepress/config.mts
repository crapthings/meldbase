import { defineConfig } from "vitepress";

/** Detects CI builds that need the GitHub Pages URL prefix. */
const isGitHubPagesBuild = process.env.GITHUB_ACTIONS === "true";

/** Configures documentation branding, navigation, and deployment paths. */
export default defineConfig({
  title: "Meldbase",
  description: "A durable embedded document database.",
  base: isGitHubPagesBuild ? "/meldbase/" : "/",
  cleanUrls: true,
  themeConfig: {
    logo: "/mark.svg",
    nav: [
      { text: "Five-minute Go tour", link: "/getting-started" },
      { text: "Browser", link: "/browser" },
      { text: "Node.js", link: "/node" },
      { text: "GitHub", link: "https://github.com/crapthings/meldbase" },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/crapthings/meldbase" }],
    footer: {
      message: "Released under the Apache License 2.0.",
      copyright: "Copyright © 2026 Meldbase contributors",
    },
  },
});
