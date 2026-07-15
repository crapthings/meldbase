# Security policy

Meldbase is an experimental developer preview. It has not received an external
security audit and must not yet be used for production secrets, regulated data,
or untrusted multi-tenant workloads.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue. Use GitHub private
vulnerability reporting for this repository. The repository owner must enable
that feature before the public launch.

Include the affected revision, reproduction steps, impact, and any suggested
mitigation. Please avoid accessing data that does not belong to you and give the
maintainers a reasonable opportunity to investigate before public disclosure.

## Supported versions

Until the first tagged release, only the current `main` branch receives security
fixes. Compatibility and storage-format stability are not yet guaranteed.

## Current security boundary

- Client queries and mutations are untrusted and are decoded into bounded,
  versioned data-only ASTs.
- Production embeddings must provide authentication and authorization policies.
- `--dev-no-auth` is local-development-only and must never be exposed publicly.
- Resume tokens are signed continuity capabilities, not authorization grants.
- Checksums detect accidental corruption; they do not authenticate or encrypt
  the database file.
