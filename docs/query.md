# Query semantics

Filters are Go maps at the public boundary and compiled before scanning. Field
paths use dot-separated object keys. Arrays are values, not implicit path fan-out.
Stored object keys cannot contain `.`, start with `$`, contain NUL, or use the
prototype-sensitive names `__proto__`, `prototype`, and `constructor`. This keeps
paths unambiguous across Go, JavaScript, indexes, and updates.

V1 operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`,
`$exists`, `$and`, `$or`, and `$not`.

Rules:

- Sibling fields and operators are combined with logical AND.
- A missing field is distinct from a present null value.
- Scalar equality against an array is true when any direct array element equals
  the scalar. Array-to-array equality is structural and ordered.
- Ordering comparisons accept comparable values only. Integers and finite floats
  compare numerically; other cross-kind comparisons are false.
- TypeScript `bigint` is Int64 and `number` is Float64. Int64 is string-framed on
  the JSON wire, so values above JavaScript's safe-integer limit remain exact.
- Time values use UTC millisecond precision, matching JavaScript `Date`; Go values
  are normalized when constructed rather than truncated during transport.
- `$not` negates one field predicate or one logical expression.
- Unknown `$` operators, invalid paths, and malformed operand shapes are errors.

These are Meldbase semantics. Differences from MongoDB are expected and will be
treated as documentation or design issues, not compatibility bugs.
