package com.project.csvanalyser.cli;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class CliParserTest {

    @Test
    void helpReturnsNull() {
        assertNull(CliParser.parse(new String[] { "--help" }));
    }

    @Test
    void helpPrintsUsage() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream prev = System.out;
        System.setOut(new PrintStream(out, false, StandardCharsets.UTF_8));
        try {
            CliParser.printHelp();
            String s = out.toString(StandardCharsets.UTF_8);
            assertTrue(s.contains("--input"));
            assertTrue(s.contains("--help"));
        } finally {
            System.setOut(prev);
        }
    }

    @Test
    void parseInputAndOutput() {
        CliConfig config = CliParser.parse(new String[] {
                "--input", "data.csv",
                "--output", "out.json"
        });
        assertNotNull(config);
        assertEquals("data.csv", config.getInputPath().toString());
        assertEquals("out.json", config.getOutputPath().toString());
        assertEquals(',', config.getDelimiter());
        assertTrue(config.isHasHeader());
        assertEquals(10, config.getTopN());
    }

    @Test
    void parseGroupByAndAgg() {
        CliConfig config = CliParser.parse(new String[] {
                "--input", "x.csv",
                "--output", "y.json",
                "--group-by", "product,region",
                "--agg", "count,sum(amount),avg(amount)"
        });
        assertNotNull(config);
        assertEquals(List.of("product", "region"), config.getGroupByColumns());
        assertEquals(List.of("count", "sum(amount)", "avg(amount)"), config.getAggregationSpecs());
    }
}
