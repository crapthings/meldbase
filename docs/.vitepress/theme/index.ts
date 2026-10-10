import DefaultTheme from "vitepress/theme";
import type { Theme } from "vitepress";
import MeldbaseHome from "./components/MeldbaseHome.vue";
import "./style.css";

/** Adds the interactive product homepage to the documentation theme. */
export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component("MeldbaseHome", MeldbaseHome);
  },
} satisfies Theme;
