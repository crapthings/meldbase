# Security policy

Meldbase is an experimental developer preview. It has not received an external
security audit and must not yet be used for production secrets, regulated data,
or untrusted multi-workspace workloads.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue. Use GitHub private
vulnerability reporting at
<https://github.com/crapthings/meldbase/security/advisories/new>. It is enabled
for the public repository and keeps the report and maintainer discussion private.

Include the affected revision, reproduction steps, impact, and any suggested
mitigation. Please avoid accessing data that does not belong to you and give the
maintainers a reasonable opportunity to investigate before public disclosure.

## Supported versions

During the alpha period, the current `main` branch and the latest published alpha
receive best-effort security fixes. Older alpha tags are unsupported after a
replacement is published. Compatibility and storage-format stability are not
yet guaranteed; release notes must identify any migration or protocol boundary.

## Current security boundary

- Client queries and mutations are untrusted and are decoded into bounded,
  versioned data-only ASTs.
- Production embeddings must provide authentication and authorization policies.
- `--dev-no-auth` is local-development-only and must never be exposed publicly.
- Resume tokens are signed continuity capabilities, not authorization grants.
- Checksums detect accidental corruption; they do not authenticate or encrypt
  the database file.
