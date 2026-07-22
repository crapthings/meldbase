import { defineConfig } from "vitepress";

const isGitHubPagesBuild = process.env.GITHUB_ACTIONS === "true";

export default defineConfig({
  title: "Meldbase",
  description: "A durable embedded document database.",
  base: isGitHubPagesBuild ? "/meldbase/" : "/",
  cleanUrls: true,
  themeConfig: {
    logo: "/mark.svg",
    nav: [
      { text: "TypeScript API", link: "/api/typescript/" },
      { text: "GitHub", link: "https://github.com/crapthings/meldbase" },
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/crapthings/meldbase" }],
    footer: {
      message: "Released under the Apache License 2.0.",
      copyright: "Copyright © 2026 Meldbase contributors",
    },
  },
});
