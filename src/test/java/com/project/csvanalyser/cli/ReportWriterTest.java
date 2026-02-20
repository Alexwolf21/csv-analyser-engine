package com.project.csvanalyser.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.csvanalyser.aggregation.AggregationState;
import com.project.csvanalyser.aggregation.GroupKey;
import com.project.csvanalyser.aggregation.TopN;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReportWriterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void writesJsonSummary(@TempDir Path dir) throws Exception {
        AggregationState state = new AggregationState();
        state.addRow(Map.of("product", "A", "amount", "100"), List.of(
                new com.project.csvanalyser.aggregation.AggregationSpec.AggregationOpWithColumn(com.project.csvanalyser.aggregation.AggregationSpec.AggregationOp.COUNT, null),
                new com.project.csvanalyser.aggregation.AggregationSpec.AggregationOpWithColumn(com.project.csvanalyser.aggregation.AggregationSpec.AggregationOp.SUM, "amount")
        ));
        state.addRow(Map.of("product", "A", "amount", "200"), List.of(
                new com.project.csvanalyser.aggregation.AggregationSpec.AggregationOpWithColumn(com.project.csvanalyser.aggregation.AggregationSpec.AggregationOp.COUNT, null),
                new com.project.csvanalyser.aggregation.AggregationSpec.AggregationOpWithColumn(com.project.csvanalyser.aggregation.AggregationSpec.AggregationOp.SUM, "amount")
        ));
        Map<GroupKey, AggregationState> stateByGroup = Map.of(new GroupKey(List.of("A")), state);
        List<TopN.TopNEntry> topN = List.of(new TopN.TopNEntry(new GroupKey(List.of("A")), 300.0));
        AnalyticsResult result = new AnalyticsResult("input.csv", 2, 0, stateByGroup, topN, List.of("product"));
        Path out = dir.resolve("summary.json");
        CliConfig config = new CliConfig(Path.of("input.csv"), null, List.of("product"),
                List.of("count", "sum(amount)"), "sum_amount", 10, out, ',', true);

        PrintStream prevOut = System.out;
        try {
            System.setOut(new PrintStream(new ByteArrayOutputStream(), false, StandardCharsets.UTF_8));
            ReportWriter.write(result, config);
        } finally {
            System.setOut(prevOut);
        }

        assertTrue(Files.exists(out));
        @SuppressWarnings("unchecked")
        Map<String, Object> json = MAPPER.readValue(out.toFile(), Map.class);
        assertEquals("input.csv", json.get("inputFile"));
        assertEquals(2, ((Number) json.get("totalRows")).intValue());
        assertEquals(0, ((Number) json.get("malformedRows")).intValue());
        List<?> groups = (List<?>) json.get("groups");
        assertNotNull(groups);
        assertEquals(1, groups.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> g = (Map<String, Object>) groups.get(0);
        assertEquals(2, ((Number) g.get("count")).intValue());
        assertEquals(300.0, ((Number) g.get("sum_amount")).doubleValue());
    }
}
