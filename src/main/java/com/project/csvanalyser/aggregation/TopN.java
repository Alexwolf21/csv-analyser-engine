package com.project.csvanalyser.aggregation;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Computes top-N groups by a metric (e.g. sum(amount)), with deterministic tie-break by group key.
 */
public final class TopN {

    /**
     * Metric name as used in output: e.g. "sum_amount", "count", "avg_amount".
     */
    public static double getMetricValue(AggregationState state, String metricName) {
        if ("count".equalsIgnoreCase(metricName)) {
            return state.getCount();
        }
        if (metricName.startsWith("sum_")) {
            String col = metricName.substring("sum_".length());
            return state.getSum(col);
        }
        if (metricName.startsWith("avg_")) {
            String col = metricName.substring("avg_".length());
            return state.getAvg(col);
        }
        if (metricName.startsWith("min_")) {
            String col = metricName.substring("min_".length());
            Double v = state.getMin(col);
            return v == null ? Double.NEGATIVE_INFINITY : v;
        }
        if (metricName.startsWith("max_")) {
            String col = metricName.substring("max_".length());
            Double v = state.getMax(col);
            return v == null ? Double.NEGATIVE_INFINITY : v;
        }
        return Double.NEGATIVE_INFINITY;
    }

    /**
     * Returns the top N entries from stateByGroup when sorted by the given metric descending, then by group key for ties.
     */
    public static List<TopNEntry> compute(Map<GroupKey, AggregationState> stateByGroup, String metricName, int n) {
        Comparator<Map.Entry<GroupKey, AggregationState>> byMetricThenKey =
                Comparator.<Map.Entry<GroupKey, AggregationState>>comparingDouble(e -> getMetricValue(e.getValue(), metricName)).reversed()
                        .thenComparing(Map.Entry::getKey);
        return stateByGroup.entrySet().stream()
                .sorted(byMetricThenKey)
                .limit(n)
                .map(e -> new TopNEntry(e.getKey(), getMetricValue(e.getValue(), metricName)))
                .toList();
    }

    public static final class TopNEntry {
        private final GroupKey groupKey;
        private final double metricValue;

        public TopNEntry(GroupKey groupKey, double metricValue) {
            this.groupKey = groupKey;
            this.metricValue = metricValue;
        }

        public GroupKey getGroupKey() {
            return groupKey;
        }

        public double getMetricValue() {
            return metricValue;
        }
    }
}
