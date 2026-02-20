package com.project.csvanalyser.cli;

import com.project.csvanalyser.aggregation.AggregationState;
import com.project.csvanalyser.aggregation.GroupKey;
import com.project.csvanalyser.aggregation.TopN;

import java.util.List;
import java.util.Map;

/**
 * Result of a CSV analytics run: counts, group aggregates, and top-N list.
 */
public final class AnalyticsResult {

    private final String inputFile;
    private final long totalRows;
    private final int malformedRows;
    private final Map<GroupKey, AggregationState> stateByGroup;
    private final List<TopN.TopNEntry> topN;
    private final List<String> groupByColumns;

    public AnalyticsResult(String inputFile, long totalRows, int malformedRows,
                           Map<GroupKey, AggregationState> stateByGroup,
                           List<TopN.TopNEntry> topN, List<String> groupByColumns) {
        this.inputFile = inputFile;
        this.totalRows = totalRows;
        this.malformedRows = malformedRows;
        this.stateByGroup = stateByGroup;
        this.topN = topN;
        this.groupByColumns = groupByColumns;
    }

    public String getInputFile() { return inputFile; }
    public long getTotalRows() { return totalRows; }
    public int getMalformedRows() { return malformedRows; }
    public Map<GroupKey, AggregationState> getStateByGroup() { return stateByGroup; }
    public List<TopN.TopNEntry> getTopN() { return topN; }
    public List<String> getGroupByColumns() { return groupByColumns; }
}
