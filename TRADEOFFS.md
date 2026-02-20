# Tradeoffs and design decisions

## Filter language

- **Choice:** A small hand-rolled expression parser supporting `==`, `!=`, `>`, `<`, `>=`, `<=`, `&&`, `||`, string literals in quotes, and unquoted numbers.
- **Tradeoff:** A full expression language (e.g. JEXL or a proper parser) would allow more complex expressions and better error messages, but would add a dependency and complexity. The current subset is enough for the required predicates and keeps the solution minimal and testable.

## Single-threaded vs multi-threaded

- **Choice:** Single-threaded pipeline: read → filter → group/aggregate in one pass.
- **Tradeoff:** Multi-threading could speed up I/O-bound or CPU-bound parsing on very large files, but would require concurrent structures (e.g. `ConcurrentHashMap` + atomic counters) and careful handling of ordering. For the timebox and “minimal working solution” goal, single-threaded streaming is simpler and correct. No `ConcurrentHashMap` is used; aggregation state is updated sequentially.

## Encoding

- **Choice:** UTF-8 is assumed for the input CSV and for the JSON output.
- **Tradeoff:** Supporting other encodings (e.g. via `--encoding`) would require passing a `Charset` through the reader; not implemented to keep the MVP small.

## Percentiles (p95, histograms)

- **Choice:** Not implemented. Only exact aggregations are supported: count, sum, avg, min, max, and top-N.
- **Tradeoff:** Exact percentiles would require storing all values per group (breaking the “do not hold all rows in memory” constraint for that group). Approximate methods (e.g. t-digest, reservoir sampling) could be added later and would need to be documented (exact vs approximate, algorithm).

## Malformed rows

- **Choice:** Rows that fail to parse (e.g. unclosed quotes) are skipped, counted in `malformedRows`, and a warning is logged (or sent to an optional sink in tests). Processing continues.
- **Tradeoff:** Recovery after a parse error is best-effort (record-by-record with quote-aware line reading). Some malformed input may leave the parser in a bad state; in practice, skipping and counting is sufficient for the required robustness.

## Determinism

- **Choice:** Group keys are ordered (lexicographic), and top-N ties are broken by group key. Output order (console and JSON) is deterministic for repeatability.
