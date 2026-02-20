package com.project.csvanalyser.filter;

import java.util.List;
import java.util.Set;

/**
 * Parses a simple filter expression and returns a predicate that can be evaluated against a row (Map of column -> value).
 * <p>
 * Supported syntax:
 * <ul>
 *   <li>Comparisons: column == "value", column != "value", column &gt; number, column &lt; number, column &gt;= number, column &lt;= number</li>
 *   <li>String literals in double quotes; numbers unquoted</li>
 *   <li>Combined with &amp;&amp; (and) and || (or)</li>
 * </ul>
 * If a column referenced in the filter is not in the header, building the predicate fails fast with a clear error.
 */
public final class FilterParser {

    /**
     * Parse the filter expression and build a RowPredicate.
     *
     * @param expression filter expression (e.g. region=="APAC" &amp;&amp; amount&gt;1000)
     * @param header     column names that exist in the CSV; used to validate column names and fail fast if missing
     * @return predicate that evaluates the expression for a given row
     * @throws IllegalArgumentException if expression is invalid or references a column not in header
     */
    public static RowPredicate parse(String expression, List<String> header) {
        if (expression == null || expression.isBlank()) {
            return row -> true;
        }
        Set<String> validColumns = header == null ? Set.of() : Set.copyOf(header);
        return new FilterExpressionParser(expression.trim(), validColumns).parse();
    }

    private FilterParser() {
    }
}
