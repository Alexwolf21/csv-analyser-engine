package com.project.csvanalyser.aggregation;

import java.util.*;
import java.util.stream.Stream;

/**
 * Consumes a stream of rows and maintains per-group aggregation state. One pass; only group state in memory.
 */
public final class StreamAggregator {

    private final List<String> groupByColumns;
    private final List<AggregationSpec.AggregationOpWithColumn> aggregations;
    private final Map<GroupKey, AggregationState> stateByGroup = new HashMap<>();

    public StreamAggregator(AggregationSpec spec) {
        this.groupByColumns = spec.getGroupByColumns();
        this.aggregations = spec.getAggregations();
    }

    public void accept(Map<String, String> row) {
        GroupKey key = keyFromRow(row);
        stateByGroup.computeIfAbsent(key, k -> new AggregationState()).addRow(row, aggregations);
    }

    public void consume(Stream<Map<String, String>> stream) {
        stream.forEach(this::accept);
    }

    private GroupKey keyFromRow(Map<String, String> row) {
        if (groupByColumns.isEmpty()) {
            return new GroupKey(List.of());
        }
        List<String> values = new ArrayList<>();
        for (String col : groupByColumns) {
            values.add(row.getOrDefault(col, ""));
        }
        return new GroupKey(values);
    }

    /**
     * Returns groups in deterministic (lexicographic) order.
     */
    public Map<GroupKey, AggregationState> getStateByGroup() {
        Map<GroupKey, AggregationState> sorted = new TreeMap<>(stateByGroup);
        return sorted;
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public List<AggregationSpec.AggregationOpWithColumn> getAggregations() {
        return aggregations;
    }
}
