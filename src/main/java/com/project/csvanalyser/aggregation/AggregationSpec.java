package com.project.csvanalyser.aggregation;

import java.util.List;

/**
 * Specifies group-by columns and which aggregations to compute (count, sum(column), avg(column), min(column), max(column)).
 */
public final class AggregationSpec {

    private final List<String> groupByColumns;
    private final List<AggregationOpWithColumn> aggregations;

    public AggregationSpec(List<String> groupByColumns, List<AggregationOpWithColumn> aggregations) {
        this.groupByColumns = groupByColumns == null ? List.of() : List.copyOf(groupByColumns);
        this.aggregations = aggregations == null ? List.of() : List.copyOf(aggregations);
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public List<AggregationOpWithColumn> getAggregations() {
        return aggregations;
    }

    public enum AggregationOp {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    }

    /** For COUNT, column is null; for SUM, AVG, MIN, MAX the column name is set. */
    public static final class AggregationOpWithColumn {
        private final AggregationOp op;
        private final String column;

        public AggregationOpWithColumn(AggregationOp op, String column) {
            this.op = op;
            this.column = column;
        }

        public AggregationOp getOp() {
            return op;
        }

        public String getColumn() {
            return column;
        }
    }
}
