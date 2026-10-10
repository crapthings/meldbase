<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { withBase } from "vitepress";
import { LocalCollection } from "../../../../sdk/client/src/local";
import type { Document } from "../../../../sdk/client/src/types";

/** Defines the typed documents used by the public in-memory demonstration. */
interface Todo extends Document {
  title: string;
  done: boolean;
}
/** Seeds a deterministic collection so the server and browser render the same page. */
const seed: Todo[] = [
  { _id: "00000000000000000000000000000001", title: "Open your first database", done: true },
  { _id: "00000000000000000000000000000002", title: "Write a document", done: false },
  { _id: "00000000000000000000000000000003", title: "Watch your query update", done: false },
];
const collection = new LocalCollection<Todo>(seed);
const documents = ref<Todo[]>(collection.find({}).fetch());
const filter = ref(false);
const results = computed(() => {
  documents.value;
  return collection.find(filter.value ? {} : { done: false }).fetch();
});
const title = ref("");
const tab = ref("Go");
const copied = ref(false);
let stop: (() => void) | undefined;
/** Subscribes to actual SDK snapshots after hydration. */
onMounted(() => {
  stop = collection.find({}).subscribe((value) => {
    documents.value = value;
  });
});
onUnmounted(() => stop?.());
/** Writes a new document through the same local SDK used by applications. */
function add() {
  if (title.value.trim()) {
    collection.insertOne({ title: title.value.trim(), done: false });
    title.value = "";
  }
}
/** Updates one document and lets the SDK notify the subscribed view. */
function toggle(todo: Todo) {
  collection.updateOne({ _id: todo._id }, { $set: { done: !todo.done } });
}
/** Restores the demonstration's original documents in one local batch. */
function reset() {
  collection.batch(() => {
    collection
      .find({})
      .fetch()
      .forEach((todo) => collection.remove(todo._id));
    seed.forEach((todo) => collection.insert(todo));
  });
}
/** Provides short API excerpts with complete runnable guides linked alongside them. */
const snippets: Record<string, string> = {
  Go: `db, err := meldbase.Open("todos.meld")\nif err != nil { return err }\ndefer db.Close()\n\ntodos := db.Collection("todos")\n_, err = todos.InsertOne(ctx, meldbase.Document{\n    "title": meldbase.String("Build something"),\n    "done":  meldbase.Bool(false),\n})\n\ncursor, err := todos.Find(ctx,\n    meldbase.Filter{"done": false})`,
  Browser: `import { MeldbaseClient } from "@meldbase/client";\n\nconst client = new MeldbaseClient({\n  baseUrl: "http://127.0.0.1:8080",\n});\nconst todos = client.collection("todos");\n\nconst stop = todos.find({ done: false })\n  .subscribe(documents => {\n    render(documents);\n  });\n\n// Call stop() when the view unmounts.`,
  "Node.js": `import WebSocket from "ws";\nimport { MeldbaseClient } from "@meldbase/client";\n\nconst client = new MeldbaseClient({\n  baseUrl: "http://127.0.0.1:8080",\n  webSocketFactory: url => new WebSocket(url),\n});\n\nconst todos = client.collection("todos");\nawait todos.insertOne({\n  title: "Process a job", done: false,\n});\nclient.close();`,
};
const guides: Record<string, string> = { Go: "/getting-started", Browser: "/browser", "Node.js": "/node" };
/** Copies the selected API excerpt after an explicit visitor action. */
async function copy() {
  try {
    await navigator.clipboard.writeText(snippets[tab.value]);
    copied.value = true;
  } catch {
    copied.value = false;
  }
}
</script>

<template>
  <main class="meld-home">
    <section class="hero wrap">
      <div class="hero-copy">
        <a class="eyebrow" :href="withBase('/overview')"><span class="status-dot" /> OPEN SOURCE · ALPHA</a>
        <h1>Your database,<br /><span>one file away.</span></h1>
        <p class="lede">
          Documents, queries, and live updates. Embed Meldbase in your Go app, or serve the same database to your
          browser and Node.js clients.
        </p>
        <div class="actions">
          <a class="button primary" :href="withBase('/getting-started')">Start with Go <span>↗</span></a
          ><a class="button secondary" :href="withBase('/overview')">Explore the docs →</a>
        </div>
        <p class="hero-note">Apache 2.0 · Single node · Built in Go</p>
      </div>
      <div class="editor">
        <div class="editor-top">
          <span class="editor-label">A small API. A real database.</span><span class="file-dot" />
        </div>
        <div class="code-tabs" role="tablist" aria-label="SDK language">
          <button
            v-for="language in Object.keys(snippets)"
            :key="language"
            role="tab"
            :aria-selected="tab === language"
            :class="{ active: tab === language }"
            @click="
              tab = language;
              copied = false;
            "
          >
            {{ language }}</button
          ><button class="copy" :aria-label="copied ? 'Code copied' : 'Copy code'" @click="copy">
            {{ copied ? "Copied" : "Copy" }}
          </button>
        </div>
        <pre role="tabpanel" :aria-label="`${tab} API excerpt`"><code>{{ snippets[tab] }}</code></pre>
        <a class="editor-footer" :href="withBase(guides[tab])"
          >{{ tab === "Go" ? "Run the complete Go example" : "Workspace SDK preview · Follow the setup guide" }}
          <span>→</span></a
        >
      </div>
    </section>
    <div class="principles wrap">
      <span><b>01</b> Own your database file</span><span><b>02</b> Query typed documents</span
      ><span><b>03</b> Subscribe to what changes</span>
    </div>
    <section class="demo-section wrap" aria-labelledby="demo-heading">
      <div class="section-heading">
        <div>
          <p class="eyebrow">TRY THE QUERY</p>
          <h2 id="demo-heading">Change a document.<br />See the result move.</h2>
        </div>
        <p>Check a todo or add your own. The result follows the query through the actual local SDK.</p>
      </div>
      <div class="demo-grid">
        <div class="demo-panel">
          <div class="panel-heading">
            <h3>Collection <span>todos</span></h3>
            <button class="text-button" @click="reset">Reset</button>
          </div>
          <ul class="todo-list">
            <li v-for="todo in documents" :key="todo._id">
              <label
                ><input type="checkbox" :checked="todo.done" @change="toggle(todo)" /><span
                  :class="{ completed: todo.done }"
                  >{{ todo.title }}</span
                ></label
              >
            </li>
          </ul>
          <form class="add-form" @submit.prevent="add">
            <label class="sr-only" for="todo-title">New todo title</label
            ><input id="todo-title" v-model="title" maxlength="80" placeholder="What will you build?" required /><button
              class="button primary"
              type="submit"
            >
              Add →
            </button>
          </form>
        </div>
        <div class="demo-panel results-panel">
          <div class="panel-heading">
            <h3>
              Live result <span aria-live="polite">{{ results.length }} documents</span>
            </h3>
            <button class="text-button" :aria-pressed="filter" @click="filter = !filter">
              {{ filter ? "Show open" : "Show all" }}
            </button>
          </div>
          <code class="query-line">todos.find({{ filter ? "{}" : "{ done: false }" }})</code>
          <div class="result-list" aria-live="polite">
            <div v-for="todo in results" :key="todo._id" class="result">
              <span class="result-dot" /><span>{{ todo.title }}</span
              ><code>{{ todo.done ? "true" : "false" }}</code>
            </div>
            <p v-if="!results.length" class="empty">All done. Add a todo to see it appear here.</p>
          </div>
        </div>
      </div>
      <p class="demo-note">
        This demo runs in memory in your browser. Refreshing resets it.
        <a :href="withBase('/browser')">Connect a real app to the server →</a>
      </p>
    </section>
    <section class="capabilities wrap">
      <p class="eyebrow">SMALL FOOTPRINT. USEFUL PRIMITIVES.</p>
      <div class="capability-grid">
        <article>
          <span class="number">01 / STORE</span>
          <h2>Keep the app close <br />to its data.</h2>
          <p>
            Open a durable .meld file from Go. Use documents and collections without running a separate database
            service.
          </p>
          <a :href="withBase('/documents')">Work with documents →</a>
        </article>
        <article>
          <span class="number">02 / QUERY</span>
          <h2>Ask for what<br />you need.</h2>
          <p>
            Filter, sort, and paginate. Add indexes for your access patterns, with explicit semantics for typed values.
          </p>
          <a :href="withBase('/queries')">Understand queries →</a>
        </article>
        <article>
          <span class="number">03 / CONNECT</span>
          <h2>Let your views <br />stay in sync.</h2>
          <p>Add the HTTP service when another process needs access. Subscribe to query results over WebSocket.</p>
          <a :href="withBase('/realtime')">Build with live queries →</a>
        </article>
      </div>
    </section>
    <section class="start-section wrap">
      <div>
        <p class="eyebrow">YOUR NEXT STEP</p>
        <h2>From first write<br />to a working app.</h2>
      </div>
      <div class="guide-list">
        <a
          v-for="guide in [
            { name: 'Go', text: 'Open a file. Write and read a document.', path: '/getting-started' },
            { name: 'Browser', text: 'Run the public realtime todo example.', path: '/browser' },
            { name: 'Node.js', text: 'Connect your service over HTTP and WebSocket.', path: '/node' },
          ]"
          :key="guide.name"
          :href="withBase(guide.path)"
          ><span
            ><b>{{ guide.name }}</b
            ><small>{{ guide.text }}</small></span
          ><span>↗</span></a
        >
      </div>
    </section>
    <aside class="alpha-note wrap">
      <b>Built in the open. Still in alpha.</b>
      <p>
        Meldbase is a single-node database. APIs and storage compatibility can change before 1.0. Review the
        <a :href="withBase('/overview')">scope and usage notes</a> before adopting it.
      </p>
      <a href="https://github.com/crapthings/meldbase">Follow on GitHub ↗</a>
    </aside>
  </main>
</template>
