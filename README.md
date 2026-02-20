# CSV Analytics Engine

A CLI application that processes large CSV files with streaming I/O: filter rows, group by columns, compute aggregations (count, sum, avg, min, max), and output a console report plus a JSON summary. **Rows are streamed; the file is not loaded entirely into memory.**

## Build

```bash
mvn clean install
```

## Run

```bash
java -jar target/csv-analytics-0.1.0-SNAPSHOT.jar --input <path> --output <path> [options]
```

### Example

```bash
java -jar target/csv-analytics-0.1.0-SNAPSHOT.jar \
  --input logs/sales.csv \
  --filter "region==\"APAC\" && amount>1000" \
  --group-by product,region \
  --agg "count,sum(amount),avg(amount),max(amount)" \
  --top-n 5 \
  --output summary.json
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--input` | Input CSV file path (required) | - |
| `--output` | Output JSON summary path (required) | - |
| `--filter` | Filter expression (see below) | (none) |
| `--group-by` | Comma-separated group columns | (none → global) |
| `--agg` | Aggregations: `count`, `sum(col)`, `avg(col)`, `min(col)`, `max(col)` | count |
| `--top-n` | Number of top groups to report | 10 |
| `--top-n-metric` | Metric for top-N: `count`, `sum_<col>`, `avg_<col>`, etc. | sum_amount |
| `--delimiter` | CSV delimiter | `,` |
| `--header` | First row is header (`true`/`false`) | true |
| `--help` | Print usage | - |

### Filter syntax

- **Comparisons:** `column == "value"`, `column != "value"`, `column > number`, `column < number`, `column >= number`, `column <= number`
- **String literals** in double quotes; **numbers** unquoted
- **Combine** with `&&` (and) and `||` (or)

Example: `region=="APAC" && amount>1000`

## Output

- **Console:** Human-readable table of groups and aggregates, plus a “TOP N” section.
- **JSON file:** `inputFile`, `totalRows`, `malformedRows`, `groups` (array of `groupKey` + aggregate fields), `topN` (array of group key + metric). Field names use underscores (e.g. `sum_amount`, `avg_amount`).

## Tests

```bash
mvn test
```

Unit tests cover CSV parsing (including quoted fields and malformed rows), filter evaluation, grouping, aggregations, top-N, CLI parsing, and JSON output.

## Tradeoffs

See [TRADEOFFS.md](TRADEOFFS.md) for design decisions (filter language, single-threaded processing, encoding, percentiles).
