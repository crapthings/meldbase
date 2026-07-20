---
layout: home

hero:
  name: Meldbase
  text: Documents that stay live, local by design
  tagline: A durable embedded document database with typed queries, realtime delivery, and an application-owned security boundary.
  actions:
    - theme: brand
      text: Get started
      link: /guide/getting-started
    - theme: alt
      text: Read the capability audit
      link: /mvp-audit

features:
  - title: Start with one file
    details: Open a local durable database from Go. Add a server only when another process or browser needs access.
  - title: Keep application data live
    details: Query once, then receive ordered realtime snapshots and deltas through the same typed contract.
  - title: Keep authority in your app
    details: The server derives workspace scope from verified JWT claims; your identity service still owns users and roles.
  - title: Operate deliberately
    details: Health probes, dashboard, metrics, backup, restore, offline verification, and a single-node runbook are built in.
---

## Choose a path

- **Build into a Go program:** follow the [getting-started guide](./guide/getting-started).
- **See a live browser UI:** run the [realtime todo example](./guide/realtime-todos).
- **Run a secured local service:** use the [single-node deployment guide](./single-node-deployment).
- **Understand the boundary:** read the [current alpha capability audit](./mvp-audit).
- **Find an API or command:** open the [reference](./reference/).

## Current alpha

Meldbase is intentionally evolving and is not yet suitable for production data.
It has one current storage format and no legacy runtime path. Read the
[roadmap](./roadmap) and [release process](./releasing) before relying on a
release for a new environment.
