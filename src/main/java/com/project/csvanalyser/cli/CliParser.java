package com.project.csvanalyser.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple hand-rolled CLI parser for --key value style arguments.
 */
public final class CliParser {

    public static final String HELP = "--help";
    public static final String INPUT = "--input";
    public static final String FILTER = "--filter";
    public static final String GROUP_BY = "--group-by";
    public static final String AGG = "--agg";
    public static final String TOP_N = "--top-n";
    public static final String OUTPUT = "--output";
    public static final String DELIMITER = "--delimiter";
    public static final String HEADER = "--header";

    private static final char DEFAULT_DELIMITER = ',';
    private static final boolean DEFAULT_HEADER = true;
    private static final int DEFAULT_TOP_N = 10;

    /**
     * @return CliConfig or null if --help was passed (caller should print help and exit).
     */
    public static CliConfig parse(String[] args) {
        for (String a : args) {
            if (HELP.equals(a)) {
                return null;
            }
        }
        Path input = getPath(args, INPUT, null);
        String filter = getString(args, FILTER, null);
        List<String> groupBy = getList(args, GROUP_BY, ',');
        List<String> agg = getList(args, AGG, ',');
        String topNMetric = getString(args, "--top-n-metric", "sum_amount");
        int topN = getInt(args, TOP_N, DEFAULT_TOP_N);
        Path output = getPath(args, OUTPUT, null);
        char delimiter = getDelimiter(args);
        boolean hasHeader = getBoolean(args, HEADER, DEFAULT_HEADER);

        return new CliConfig(input, filter, groupBy, agg, topNMetric, topN, output, delimiter, hasHeader);
    }

    public static void printHelp() {
        System.out.println("CSV Analytics Engine");
        System.out.println();
        System.out.println("Usage: java -jar csv-analytics.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --input <path>       Input CSV file (required)");
        System.out.println("  --filter <expr>      Filter expression (e.g. region==\"APAC\" && amount>1000)");
        System.out.println("  --group-by <cols>     Comma-separated group columns (e.g. product,region)");
        System.out.println("  --agg <spec>         Comma-separated aggregations: count, sum(col), avg(col), min(col), max(col)");
        System.out.println("  --top-n <n>          Number of top groups to report (default: 10)");
        System.out.println("  --top-n-metric <name> Metric for top-N: count, sum_<col>, avg_<col>, etc. (default: sum_amount)");
        System.out.println("  --output <path>       Output JSON summary path (required)");
        System.out.println("  --delimiter <char>   CSV delimiter (default: ,)");
        System.out.println("  --header <true|false> First row is header (default: true)");
        System.out.println("  --help               Print this message");
    }

    private static String getString(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    private static Path getPath(String[] args, String key, Path defaultValue) {
        String s = getString(args, key, null);
        return s == null ? defaultValue : Path.of(s);
    }

    private static List<String> getList(String[] args, String key, char sep) {
        String s = getString(args, key, null);
        if (s == null || s.isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String part : s.split(String.valueOf(sep))) {
            out.add(part.trim());
        }
        return out;
    }

    private static int getInt(String[] args, String key, int defaultValue) {
        String s = getString(args, key, null);
        if (s == null) return defaultValue;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static char getDelimiter(String[] args) {
        String s = getString(args, DELIMITER, null);
        if (s == null || s.isEmpty()) return DEFAULT_DELIMITER;
        return s.charAt(0);
    }

    private static boolean getBoolean(String[] args, String key, boolean defaultValue) {
        String s = getString(args, key, null);
        if (s == null) return defaultValue;
        return "true".equalsIgnoreCase(s.trim()) || "1".equals(s.trim());
    }
}
