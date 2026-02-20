package com.project.csvanalyser.aggregation;

import java.util.HashMap;
import java.util.Map;

/**
 * Running state for one group: count and numeric aggregates (sum, min, max). Avg = sum/count at end.
 */
public final class AggregationState {

    private long count;
    private final Map<String, Double> sumByColumn = new HashMap<>();
    private final Map<String, Double> minByColumn = new HashMap<>();
    private final Map<String, Double> maxByColumn = new HashMap<>();

    public void addRow(Map<String, String> row, Iterable<AggregationSpec.AggregationOpWithColumn> numericAggs) {
        count++;
        for (AggregationSpec.AggregationOpWithColumn a : numericAggs) {
            if (a.getColumn() == null) continue;
            String raw = row.getOrDefault(a.getColumn(), "").trim();
            if (raw.isEmpty()) continue;
            double value;
            try {
                value = Double.parseDouble(raw);
            } catch (NumberFormatException e) {
                continue;
            }
            switch (a.getOp()) {
                case SUM, AVG -> sumByColumn.merge(a.getColumn(), value, Double::sum);
                case MIN -> minByColumn.merge(a.getColumn(), value, (x, y) -> Math.min(x, y));
                case MAX -> maxByColumn.merge(a.getColumn(), value, (x, y) -> Math.max(x, y));
                default -> { }
            }
        }
    }

    public long getCount() {
        return count;
    }

    public double getSum(String column) {
        return sumByColumn.getOrDefault(column, 0.0);
    }

    public double getAvg(String column) {
        return count == 0 ? 0 : getSum(column) / count;
    }

    public Double getMin(String column) {
        return minByColumn.get(column);
    }

    public Double getMax(String column) {
        return maxByColumn.get(column);
    }
}
