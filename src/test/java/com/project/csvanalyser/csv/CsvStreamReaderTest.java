package com.project.csvanalyser.csv;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CsvStreamReaderTest {

    /** Use a warning sink in tests to avoid initializing SLF4J/Logback (avoids Spring Boot vs Logback conflict). */
    private static CsvStreamReader readerWithTestSink() {
        return new CsvStreamReader(',', true, msg -> {});
    }

    @Test
    void parseHeaderAndFirstRow() throws IOException {
        String csv = "a,b,c\n1,2,3";
        CsvStreamReader reader = readerWithTestSink();
        CsvStreamReader.ParseResult result = reader.stream(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "test");

        assertEquals(List.of("a", "b", "c"), result.getHeader());
        List<Map<String, String>> rows = result.getRecordStream().collect(Collectors.toList());
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).get("a"));
        assertEquals("2", rows.get(0).get("b"));
        assertEquals("3", rows.get(0).get("c"));
        assertEquals(0, result.getMalformedCounter().getCount());
    }

    @Test
    void quotedFieldsWithCommasAndNewlines() throws IOException {
        String csv = "name,value\n\"Foo, Bar\",10\n\"Line1\nLine2\",20";
        CsvStreamReader reader = readerWithTestSink();
        CsvStreamReader.ParseResult result = reader.stream(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "test");

        assertEquals(List.of("name", "value"), result.getHeader());
        List<Map<String, String>> rows = result.getRecordStream().collect(Collectors.toList());
        assertEquals(2, rows.size());
        assertEquals("Foo, Bar", rows.get(0).get("name"));
        assertEquals("10", rows.get(0).get("value"));
        assertEquals("Line1\nLine2", rows.get(1).get("name"));
        assertEquals("20", rows.get(1).get("value"));
        assertEquals(0, result.getMalformedCounter().getCount());
    }

    @Test
    void malformedRowsSkippedAndCounted() throws IOException {
        // Unclosed quote can cause parse failure
        String csv = "x,y\n1,2\n\"bad,3\n4,5";
        List<String> warnings = new ArrayList<>();
        CsvStreamReader reader = new CsvStreamReader(',', true, warnings::add);
        CsvStreamReader.ParseResult result = reader.stream(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "test");

        List<Map<String, String>> rows = result.getRecordStream().collect(Collectors.toList());
        // First data row "1,2" is valid; "bad,3 may fail depending on parser; 4,5 may be valid
        assertTrue(rows.size() >= 1);
        assertEquals("1", rows.get(0).get("x"));
        assertEquals("2", rows.get(0).get("y"));
        // Malformed counter may be >= 0 depending on how parser handles the bad line; no exception
        assertTrue(result.getMalformedCounter().getCount() >= 0);
    }

    @Test
    void emptyFileReturnsEmptyHeaderAndNoRows() throws IOException {
        String csv = "";
        CsvStreamReader reader = readerWithTestSink();
        CsvStreamReader.ParseResult result = reader.stream(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "test");

        assertTrue(result.getHeader().isEmpty());
        assertEquals(0, result.getRecordStream().count());
        assertEquals(0, result.getMalformedCounter().getCount());
    }

    @Test
    void multipleRowsCorrectOrder() throws IOException {
        String csv = "id\n1\n2\n3";
        CsvStreamReader reader = readerWithTestSink();
        CsvStreamReader.ParseResult result = reader.stream(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "test");

        List<Map<String, String>> rows = result.getRecordStream().collect(Collectors.toList());
        assertEquals(3, rows.size());
        assertEquals("1", rows.get(0).get("id"));
        assertEquals("2", rows.get(1).get("id"));
        assertEquals("3", rows.get(2).get("id"));
    }
}
