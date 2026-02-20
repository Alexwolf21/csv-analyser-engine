package com.project.csvanalyser.cli;

import com.project.csvanalyser.aggregation.AggregationSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses aggregation spec strings like "count,sum(amount),avg(amount),max(amount)" into AggregationOpWithColumn list.
 */
public final class AggregationSpecParser {

    private static final Pattern FUNC_PATTERN = Pattern.compile("(sum|avg|min|max)\\s*\\(\\s*([a-zA-Z0-9_]+)\\s*\\)");

    /**
     * @param spec   comma-separated list: count, sum(col), avg(col), min(col), max(col)
     * @param header valid column names; used to fail fast if column is missing
     */
    public static List<AggregationSpec.AggregationOpWithColumn> parse(String spec, Set<String> header) {
        if (spec == null || spec.isBlank()) {
            return List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null));
        }
        List<AggregationSpec.AggregationOpWithColumn> out = new ArrayList<>();
        for (String part : spec.split(",")) {
            String p = part.trim().toLowerCase();
            if (p.equals("count")) {
                out.add(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null));
                continue;
            }
            Matcher m = FUNC_PATTERN.matcher(part.trim());
            if (m.matches()) {
                String op = m.group(1).toLowerCase();
                String col = m.group(2);
                if (header != null && !header.contains(col)) {
                    throw new IllegalArgumentException("Unknown column in aggregation: '" + col + "'. Available: " + header);
                }
                AggregationSpec.AggregationOp opEnum = switch (op) {
                    case "sum" -> AggregationSpec.AggregationOp.SUM;
                    case "avg" -> AggregationSpec.AggregationOp.AVG;
                    case "min" -> AggregationSpec.AggregationOp.MIN;
                    case "max" -> AggregationSpec.AggregationOp.MAX;
                    default -> throw new IllegalArgumentException("Unknown aggregation: " + op);
                };
                out.add(new AggregationSpec.AggregationOpWithColumn(opEnum, col));
            }
        }
        if (out.isEmpty()) {
            out.add(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null));
        }
        return out;
    }

    /**
     * Convert metric for top-N to the name used in output (e.g. sum(amount) -> sum_amount).
     */
    public static String toMetricName(String aggSpec) {
        if (aggSpec == null || aggSpec.isBlank()) return "count";
        Matcher m = FUNC_PATTERN.matcher(aggSpec.trim());
        if (m.matches()) {
            return m.group(1).toLowerCase() + "_" + m.group(2);
        }
        if (aggSpec.trim().equalsIgnoreCase("count")) return "count";
        return aggSpec.trim();
    }
}
