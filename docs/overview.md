<script setup>
import { withBase } from "vitepress";
</script>

# Meet Meldbase

Meldbase is a single-node document database written in Go. An embedded Go
application owns a durable `.meld` file. The optional server exposes that
same database to browser and Node.js clients over HTTP and WebSocket.

## Choose your path

| You want to…                               | Start here                              |
| ------------------------------------------ | --------------------------------------- |
| Store data inside a Go application         | [Five-minute Go tour](/getting-started) |
| Connect a browser and watch results change | [Browser quickstart](/browser)          |
| Connect another backend process            | [Node.js quickstart](/node)             |
| Find the right installation route          | [Installation](/installation)           |

## A small set of useful primitives

Collections hold [typed documents](/documents). [Queries](/queries) filter,
sort, and paginate them. [Indexes](/indexes) support access patterns.
[Live queries](/realtime) deliver updated results when matching data changes.

Use `meldbase.Open(path)` for durable storage or `meldbase.New()` for an
in-memory Go database. The TypeScript SDK also has an in-memory local collection;
that is what powers the homepage demonstration.

## Scope and compatibility

Meldbase is alpha software. Public APIs, SDKs, and storage compatibility may
change before 1.0. Review the repository's
[release notes](https://github.com/crapthings/meldbase/releases) before upgrading.
Keep backups and verify recovery against the version you intend to run.

One writable process owns each durable file. Other processes connect through
the server. Distributed replication and a multi-node cluster are outside the
current product scope.

The Go module and CLI releases are available today. The TypeScript SDKs are
published as `0.1.0-beta.2` on npm under the `beta` tag.
See [SDK compatibility and release status](/sdk-status) for the reviewed public
API boundary and release evidence.

## Reference

Browse the generated <a :href="withBase('/api/typescript/')" target="_self">TypeScript API reference</a> for exact
client, React, and worker SDK signatures. The repository contains the Go API,
server implementation, CLI help, and release history.
