package com.project.csvanalyser.cli;

import com.project.csvanalyser.aggregation.*;
import com.project.csvanalyser.csv.CsvStreamReader;
import com.project.csvanalyser.filter.FilterParser;
import com.project.csvanalyser.filter.RowPredicate;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Runs the CSV analytics pipeline: stream parse -> filter -> group+aggregate -> topN.
 */
public final class CsvAnalyticsRunner {

    /**
     * Validates config and runs the pipeline. Fails fast if input missing, file not found, or column not in header.
     */
    public static AnalyticsResult run(CliConfig config) throws IOException {
        if (config.getInputPath() == null || !Files.isRegularFile(config.getInputPath())) {
            throw new IllegalArgumentException("Input file is required and must exist: " + config.getInputPath());
        }
        if (config.getOutputPath() == null) {
            throw new IllegalArgumentException("Output path is required");
        }

        CsvStreamReader reader = new CsvStreamReader(config.getDelimiter(), config.isHasHeader());
        CsvStreamReader.ParseResult parseResult = reader.stream(config.getInputPath());
        List<String> header = parseResult.getHeader();

        if (header.isEmpty()) {
            throw new IllegalArgumentException("CSV has no header. Use --header false if the file has no header row.");
        }
        Set<String> headerSet = Set.copyOf(header);

        for (String col : config.getGroupByColumns()) {
            if (!headerSet.contains(col)) {
                throw new IllegalArgumentException("Group-by column not in CSV: '" + col + "'. Available: " + header);
            }
        }

        RowPredicate filter = FilterParser.parse(config.getFilterExpression(), header);

        String aggSpecStr = String.join(",", config.getAggregationSpecs());
        List<AggregationSpec.AggregationOpWithColumn> aggList = AggregationSpecParser.parse(aggSpecStr, headerSet);
        AggregationSpec spec = new AggregationSpec(config.getGroupByColumns(), aggList);
        StreamAggregator aggregator = new StreamAggregator(spec);

        AtomicLong totalParsedRows = new AtomicLong(0);
        try (Stream<Map<String, String>> stream = parseResult.getRecordStream()) {
            stream.peek(row -> totalParsedRows.incrementAndGet())
                    .filter(filter::test)
                    .forEach(aggregator::accept);
        }

        int malformed = parseResult.getMalformedCounter().getCount();
        long totalRows = totalParsedRows.get() + malformed;

        Map<GroupKey, AggregationState> stateByGroup = aggregator.getStateByGroup();
        String topNMetric = config.getTopNMetric();
        List<TopN.TopNEntry> topNList = TopN.compute(stateByGroup, topNMetric, config.getTopN());

        return new AnalyticsResult(
                config.getInputPath().toString(),
                totalRows,
                malformed,
                stateByGroup,
                topNList,
                config.getGroupByColumns()
        );
    }
}