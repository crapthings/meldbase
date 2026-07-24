# Embedded admin API

`meld serve --admin-addr 127.0.0.1:9091` enables an independently authenticated
read-only administration surface. It is for operators, dashboards, and
monitoring agents—not application clients. The listener must bind to loopback;
place any remote access behind an application-owned authenticated TLS boundary.

Set `MELDBASE_ADMIN_TOKEN` to a unique value of at least 32 bytes before
starting the service. All API endpoints require:

```http
Authorization: Bearer <MELDBASE_ADMIN_TOKEN>
```

The API is versioned at `/v1`. Successful JSON endpoints send
`Cache-Control: no-store`; failed requests use a small fixed error-code body and
do not reveal database paths, configuration, query data, or credentials.

| Endpoint | Enablement | Response |
| --- | --- | --- |
| `GET /v1/stats` | admin listener | Latest database/server sample |
| `GET /v1/stats/history` | admin listener | Bounded sample history |
| `GET /v1/stats/stream` | admin listener | Server-sent event stream of samples |
| `GET /v1/diagnostics?after=&limit=` | `--admin-diagnostics` | Bounded slow/failure diagnostic events |
| `GET /v1/indexes` | admin listener | Published index definitions only |
| `GET /metrics` | `--admin-metrics` | Prometheus text exposition |

`/` and `/assets/*` serve the embedded dashboard when the listener is enabled.
They are intentionally separate from the public data API and cannot mutate
documents, access policy, credentials, indexes, backups, or service
configuration. Use the reviewed CLI and normal deployment workflow for those
operations.

## Browser access and auditing

Requests without an `Origin` header are allowed after bearer authentication,
which suits local agents. Browser requests must come from the exact same admin
origin or an explicitly configured allowed origin; preflight allows only
`GET` and the `Authorization` header. Admin requests produce safe JSONL audit
events containing timestamp, method, path without query parameters, status, and
outcome—never the token, headers, response, client address, or query string.

Diagnostics are intentionally bounded and opt-in. `--admin-diagnostics-all` is
appropriate only for a short development session; it should not be used as a
production request-data recorder. See [observability](/observability) and
[security](/production-security) for metrics and retention guidance.
