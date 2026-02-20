package com.project.csvanalyser.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.project.csvanalyser.aggregation.AggregationState;
import com.project.csvanalyser.aggregation.GroupKey;
import com.project.csvanalyser.aggregation.TopN;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Writes human-readable console report and machine-readable JSON summary.
 */
public final class ReportWriter {

    private static final Pattern FUNC_PATTERN = Pattern.compile("(sum|avg|min|max)\\s*\\(\\s*([a-zA-Z0-9_]+)\\s*\\)");
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static void write(AnalyticsResult result, CliConfig config) throws IOException {
        String reportText = buildReportText(result, config);
        System.out.print(reportText);
        Path reportPath = config.getReportPath();
        if (reportPath != null) {
            String pathLower = reportPath.toString().toLowerCase();
            if (pathLower.endsWith(".pdf")) {
                PdfReportWriter.write(reportText, reportPath);
            } else {
                Files.writeString(reportPath, reportText);
            }
        }
        writeJsonSummary(result, config);
    }

    /**
     * Builds the human-readable report as a single string (same content as printed to console).
     */
    public static String buildReportText(AnalyticsResult result, CliConfig config) {
        StringBuilder out = new StringBuilder();
        List<String> groupCols = result.getGroupByColumns();
        Map<GroupKey, AggregationState> stateByGroup = result.getStateByGroup();
        List<String> aggSpecs = config.getAggregationSpecs();
        if (aggSpecs.isEmpty()) {
            aggSpecs = List.of("count");
        }

        for (Map.Entry<GroupKey, AggregationState> e : stateByGroup.entrySet()) {
            GroupKey key = e.getKey();
            AggregationState state = e.getValue();
            StringBuilder keyPart = new StringBuilder("GROUP: ");
            if (groupCols.isEmpty()) {
                keyPart.append("(global)");
            } else {
                List<String> parts = new ArrayList<>();
                for (int i = 0; i < groupCols.size(); i++) {
                    String col = groupCols.get(i);
                    String val = i < key.getValues().size() ? key.getValues().get(i) : "";
                    parts.add(col + "=" + val);
                }
                keyPart.append(String.join(", ", parts));
            }
            out.append(keyPart).append("\n");
            out.append("count: ").append(state.getCount()).append("\n");
            for (String spec : aggSpecs) {
                String s = spec.trim();
                if (s.equalsIgnoreCase("count")) continue;
                Matcher m = FUNC_PATTERN.matcher(s);
                if (m.matches()) {
                    String op = m.group(1).toLowerCase();
                    String col = m.group(2);
                    String label = op + "(" + col + ")";
                    if ("sum".equals(op)) out.append(label).append(": ").append(formatNum(state.getSum(col))).append("\n");
                    else if ("avg".equals(op)) out.append(label).append(": ").append(formatNum(state.getAvg(col))).append("\n");
                    else if ("min".equals(op)) out.append(label).append(": ").append(state.getMin(col) != null ? formatNum(state.getMin(col)) : "-").append("\n");
                    else if ("max".equals(op)) out.append(label).append(": ").append(state.getMax(col) != null ? formatNum(state.getMax(col)) : "-").append("\n");
                }
            }
            out.append("---\n");
        }

        List<TopN.TopNEntry> topN = result.getTopN();
        if (!topN.isEmpty()) {
            out.append("TOP ").append(topN.size()).append(" (by ").append(config.getTopNMetric()).append("):\n");
            for (int i = 0; i < topN.size(); i++) {
                TopN.TopNEntry entry = topN.get(i);
                StringBuilder line = new StringBuilder((i + 1) + ". ");
                if (groupCols.isEmpty()) {
                    line.append("(global)");
                } else {
                    List<String> parts = new ArrayList<>();
                    for (int j = 0; j < groupCols.size(); j++) {
                        String val = j < entry.getGroupKey().getValues().size() ? entry.getGroupKey().getValues().get(j) : "";
                        parts.add(val);
                    }
                    line.append(String.join(" — ", parts));
                }
                line.append(" — ").append(formatNum(entry.getMetricValue()));
                out.append(line).append("\n");
            }
        }
        return out.toString();
    }

    private static String formatNum(double d) {
        if (d == (long) d) return String.valueOf((long) d);
        return String.format("%.2f", d);
    }

    private static void writeJsonSummary(AnalyticsResult result, CliConfig config) throws IOException {
        List<String> groupCols = result.getGroupByColumns();
        List<String> aggSpecs = config.getAggregationSpecs();
        if (aggSpecs.isEmpty()) aggSpecs = List.of("count");

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("inputFile", result.getInputFile());
        root.put("totalRows", result.getTotalRows());
        root.put("malformedRows", result.getMalformedRows());

        List<Map<String, Object>> groups = new ArrayList<>();
        for (Map.Entry<GroupKey, AggregationState> e : result.getStateByGroup().entrySet()) {
            Map<String, Object> g = new LinkedHashMap<>();
            GroupKey key = e.getKey();
            AggregationState state = e.getValue();
            Map<String, String> groupKeyMap = new LinkedHashMap<>();
            for (int i = 0; i < groupCols.size(); i++) {
                String col = groupCols.get(i);
                String val = i < key.getValues().size() ? key.getValues().get(i) : "";
                groupKeyMap.put(col, val);
            }
            g.put("groupKey", groupKeyMap);
            g.put("count", state.getCount());
            for (String spec : aggSpecs) {
                String s = spec.trim();
                if (s.equalsIgnoreCase("count")) continue;
                Matcher m = FUNC_PATTERN.matcher(s);
                if (m.matches()) {
                    String op = m.group(1).toLowerCase();
                    String col = m.group(2);
                    String field = op + "_" + col;
                    if ("sum".equals(op)) g.put(field, state.getSum(col));
                    else if ("avg".equals(op)) g.put(field, state.getAvg(col));
                    else if ("min".equals(op)) g.put(field, state.getMin(col));
                    else if ("max".equals(op)) g.put(field, state.getMax(col));
                }
            }
            groups.add(g);
        }
        root.put("groups", groups);

        List<Map<String, Object>> topNList = new ArrayList<>();
        for (TopN.TopNEntry entry : result.getTopN()) {
            Map<String, Object> t = new LinkedHashMap<>();
            for (int i = 0; i < groupCols.size(); i++) {
                String col = groupCols.get(i);
                String val = i < entry.getGroupKey().getValues().size() ? entry.getGroupKey().getValues().get(i) : "";
                t.put(col, val);
            }
            t.put(config.getTopNMetric(), entry.getMetricValue());
            topNList.add(t);
        }
        root.put("topN", topNList);

        Files.writeString(config.getOutputPath(), JSON.writeValueAsString(root));
    }
}
