package com.project.csvanalyser.filter;

import java.util.Map;

/**
 * Predicate that evaluates a filter expression against a CSV row (column name -> value).
 */
@FunctionalInterface
public interface RowPredicate {

    boolean test(Map<String, String> row);
}
