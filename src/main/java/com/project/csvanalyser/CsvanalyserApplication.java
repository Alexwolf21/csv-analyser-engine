package com.project.csvanalyser;

import com.project.csvanalyser.cli.AnalyticsResult;
import com.project.csvanalyser.cli.CliConfig;
import com.project.csvanalyser.cli.CliParser;
import com.project.csvanalyser.cli.CsvAnalyticsRunner;
import com.project.csvanalyser.cli.ReportWriter;

public class CsvanalyserApplication {

	public static void main(String[] args) {
		CliConfig config = CliParser.parse(args);
		if (config == null) {
			CliParser.printHelp();
			return;
		}
		try {
			AnalyticsResult result = CsvAnalyticsRunner.run(config);
			ReportWriter.write(result, config);
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(1);
		}
	}
}
