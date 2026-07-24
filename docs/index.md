---
layout: home

hero:
  name: Meldbase
  text: A document database that starts with one file
  tagline: Use it directly from Go, or add a small HTTP service when another process needs access.
  actions:
    - theme: brand
      text: Start in five minutes
      link: /getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/crapthings/meldbase

features:
  - title: One durable file
    details: Open a file from Go. There is no separate database service for the embedded path.
  - title: Add a server only when needed
    details: Run meld serve to expose the same database to a browser or another process.
  - title: Start small
    details: Documents, queries, indexes, and live updates are available without operating a distributed system.
---

## Where to go next

- [Five-minute Go tour](/getting-started) — create a database file, write a
  document, query it, and reopen it.
- [Browser](/browser) — connect a web application to a running `meld serve`.
- [Node.js](/node) — connect another server process to a running `meld serve`.

Meldbase is currently alpha software. Keep it local while evaluating it; do not
put irreplaceable production data in it yet.
