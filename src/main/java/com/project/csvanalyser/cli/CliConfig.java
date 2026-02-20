package com.project.csvanalyser.cli;

import java.nio.file.Path;
import java.util.List;

/**
 * Parsed CLI configuration for the CSV analytics run.
 */
public final class CliConfig {

    private final Path inputPath;
    private final String filterExpression;
    private final List<String> groupByColumns;
    private final List<String> aggregationSpecs;
    private final String topNMetric;
    private final int topN;
    private final Path outputPath;
    private final char delimiter;
    private final boolean hasHeader;

    public CliConfig(Path inputPath, String filterExpression, List<String> groupByColumns,
                    List<String> aggregationSpecs, String topNMetric, int topN, Path outputPath,
                    char delimiter, boolean hasHeader) {
        this.inputPath = inputPath;
        this.filterExpression = filterExpression;
        this.groupByColumns = groupByColumns == null ? List.of() : List.copyOf(groupByColumns);
        this.aggregationSpecs = aggregationSpecs == null ? List.of() : List.copyOf(aggregationSpecs);
        this.topNMetric = topNMetric;
        this.topN = topN;
        this.outputPath = outputPath;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
    }

    public Path getInputPath() { return inputPath; }
    public String getFilterExpression() { return filterExpression; }
    public List<String> getGroupByColumns() { return groupByColumns; }
    public List<String> getAggregationSpecs() { return aggregationSpecs; }
    public String getTopNMetric() { return topNMetric; }
    public int getTopN() { return topN; }
    public Path getOutputPath() { return outputPath; }
    public char getDelimiter() { return delimiter; }
    public boolean isHasHeader() { return hasHeader; }
}
