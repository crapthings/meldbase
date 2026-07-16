# Compound index contract

Compound indexes are a Storage V2 and in-memory capability. Legacy V1 remains
readable and keeps its frozen single-ascending-field catalog; it rejects new
compound or descending definitions rather than persisting an ambiguous record.

## Definition

- An index contains one to four distinct, valid document paths.
- Each field direction is `1` (ascending) or `-1` (descending).
- Existing one-field ascending indexes retain the V2 scalar-key codec and their
  exact on-disk bytes.
- A definition using multiple fields or descending order uses compound-key
  codec V3 and negotiates a required V2 format feature before publication.
- A missing first component omits the document, matching the existing
  single-field behavior. If a later component is missing, V3 stores the longest
  present left prefix followed by an internal partial marker and document ID.
  This makes a query such as `{a: 1}` include `{a: 1}` documents that lack `b`;
  an index must never change predicate membership.
- Every present component before that first gap must be scalar and supported by
  the existing total-order codec. Fields after a gap are irrelevant until the
  gap is filled by a later update.
- `unique` constrains the complete tuple, not any individual prefix.
  Partial keys contain the document ID, so multiple documents missing a suffix
  do not conflict with one another.

## Key encoding

Each scalar first uses the existing exact scalar codec. Codec V3 then frames
every component independently and concatenates the frames:

- ascending: escape `00` as `00 ff`, terminate with `00 00`;
- descending: complement every scalar byte, escape complemented `ff` as
  `ff 00`, terminate with `ff ff`.

The ascending frame preserves scalar byte order and makes a proper prefix sort
before its extension. The descending dual reverses both differing-byte and
proper-prefix order. Consequently the concatenation has lexicographic tuple
order for arbitrary mixed directions, while an equality tuple prefix remains a
literal byte prefix suitable for bounded B+Tree scans.

The complete encoded tuple must fit the existing Secondary-key payload bound;
resource accounting uses these canonical bytes. Field count, metadata bytes,
query nodes and catch-up batches remain independently bounded.

The reserved partial marker is lower than every scalar type tag and cannot be
confused with a framed component. Partial entries therefore remain inside the
literal equality-prefix interval but outside exact complete-tuple bounds.
Residual predicate evaluation rejects them from ranges that constrain the
missing component.

## Planner contract

The first planner version may use a compound index only for a contiguous left
prefix: zero or more equality fields followed by at most one range field.
Residual predicates are always rechecked against the resolved Primary document.
Sort coverage is an optimization, not a correctness requirement; until proved,
the existing stable result sorter remains authoritative.

## Compatibility and crash safety

V2 IndexCatalog, CreateIndex Commit Log events and persistent shadow-build
records carry the ordered field list and codec version. Old single-field records
decode unchanged. A database that has published codec V3 sets a required Meta
feature bit, so an older reader reports unsupported format before interpreting
the catalog. Build scan, catch-up, final publication, backup, reclamation and
verification must all preserve the same definition and recompute the same tuple
before this capability is considered complete.
