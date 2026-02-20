package com.project.csvanalyser.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Streams CSV records one-by-one without loading the entire file into memory.
 * Handles malformed rows by skipping them, counting, and logging a warning.
 */
public final class CsvStreamReader {

    private final char delimiter;
    private final boolean hasHeader;
    private final Consumer<String> warningSink;

    public CsvStreamReader(char delimiter, boolean hasHeader) {
        this(delimiter, hasHeader, null);
    }

    /**
     * @param warningSink if non-null, malformed row warnings are sent here; otherwise SLF4J is used.
     */
    public CsvStreamReader(char delimiter, boolean hasHeader, Consumer<String> warningSink) {
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
        this.warningSink = warningSink;
    }

    /**
     * Opens the CSV at the given path and returns the header (if present) and a stream of row maps.
     * Each map keys column names to cell values. Caller must close the stream (or consume fully) to release resources.
     *
     * @param path path to UTF-8 CSV file
     * @return result containing column names and stream of records; malformed count updated as stream is consumed
     */
    public ParseResult stream(Path path) throws IOException {
        BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        return streamFromReader(reader, path.toString());
    }

    /**
     * Streams CSV from the given input stream. Useful for tests.
     */
    public ParseResult stream(InputStream inputStream, String sourceName) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        return streamFromReader(reader, sourceName);
    }

    private ParseResult streamFromReader(BufferedReader reader, String sourceName) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setTrim(true)
                .setIgnoreEmptyLines(false)
                .build();

        // Read header
        String firstLine = reader.readLine();
        if (firstLine == null) {
            reader.close();
            return new ParseResult(List.of(), Stream.empty(), new MalformedCounter());
        }

        List<String> header;
        try {
            CSVRecord headerRecord = CSVParser.parse(firstLine, format).getRecords().get(0);
            header = new ArrayList<>();
            for (int i = 0; i < headerRecord.size(); i++) {
                header.add(headerRecord.get(i));
            }
        } catch (Exception e) {
            reader.close();
            throw new IOException("Failed to parse CSV header: " + firstLine, e);
        }

        MalformedCounter malformedCounter = new MalformedCounter();
        Iterator<Map<String, String>> recordIterator = new RecordIterator(reader, format, header, malformedCounter, sourceName, warningSink);

        Iterable<Map<String, String>> iterable = () -> recordIterator;
                Stream<Map<String, String>> stream = StreamSupport.stream(iterable.spliterator(), false)
                .onClose(() -> {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        Consumer<String> sink = warningSink;
                        if (sink != null) {
                            sink.accept("Error closing reader: " + e.getMessage());
                        } else {
                            LoggerFactory.getLogger(CsvStreamReader.class).warn("Error closing reader: {}", e.getMessage());
                        }
                    }
                });

        return new ParseResult(header, stream, malformedCounter);
    }

    private static final class RecordIterator implements Iterator<Map<String, String>> {
        private final BufferedReader reader;
        private final CSVFormat format;
        private final List<String> header;
        private final MalformedCounter malformedCounter;
        private final String sourceName;
        private final Consumer<String> warningSink;
        private Map<String, String> next;

        RecordIterator(BufferedReader reader, CSVFormat format, List<String> header,
                       MalformedCounter malformedCounter, String sourceName, Consumer<String> warningSink) {
            this.reader = reader;
            this.format = format;
            this.header = header;
            this.malformedCounter = malformedCounter;
            this.sourceName = sourceName;
            this.warningSink = warningSink;
            this.next = advance();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Map<String, String> next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            Map<String, String> current = next;
            next = advance();
            return current;
        }

        private Map<String, String> advance() {
            String recordLine = readOneRecord(reader);
            if (recordLine == null) {
                return null;
            }
            if (recordLine.isEmpty()) {
                return advance();
            }
            try {
                List<CSVRecord> records = CSVParser.parse(recordLine, format).getRecords();
                if (records.isEmpty()) {
                    return advance();
                }
                CSVRecord rec = records.get(0);
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 0; i < header.size(); i++) {
                    String value = i < rec.size() ? rec.get(i) : "";
                    row.put(header.get(i), value);
                }
                return row;
            } catch (Exception e) {
                malformedCounter.increment();
                String msg = sourceName + ": Skipping malformed row #" + malformedCounter.getCount() + ": " + (recordLine.length() > 100 ? recordLine.substring(0, 100) + "..." : recordLine);
                if (this.warningSink != null) {
                    this.warningSink.accept(msg);
                } else {
                    LoggerFactory.getLogger(CsvStreamReader.class).warn(msg);
                }
                return advance();
            }
        }

        /**
         * Reads one CSV record from the reader, handling quoted newlines.
         * Returns null at EOF.
         */
        private String readOneRecord(BufferedReader reader) {
            try {
                StringBuilder sb = new StringBuilder();
                boolean inQuotes = false;
                int c;
                while ((c = reader.read()) != -1) {
                    char ch = (char) c;
                    if (ch == '"') {
                        inQuotes = !inQuotes;
                        sb.append(ch);
                    } else if (ch == '\n' && !inQuotes) {
                        break;
                    } else if (ch == '\r') {
                        int nextChar = reader.read();
                        if (nextChar != '\n' && nextChar != -1) {
                            sb.append(ch);
                            if (nextChar != -1) {
                                sb.append((char) nextChar);
                            }
                        }
                        if (!inQuotes) break;
                    } else {
                        sb.append(ch);
                    }
                }
                if (sb.length() == 0 && c == -1) {
                    return null;
                }
                return sb.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Mutable counter for malformed rows; shared with the stream so it can be read after consumption.
     */
    public static final class MalformedCounter {
        private int count;

        void increment() {
            count++;
        }

        public int getCount() {
            return count;
        }
    }

    public static final class ParseResult {
        private final List<String> header;
        private final Stream<Map<String, String>> recordStream;
        private final MalformedCounter malformedCounter;

        public ParseResult(List<String> header, Stream<Map<String, String>> recordStream, MalformedCounter malformedCounter) {
            this.header = header;
            this.recordStream = recordStream;
            this.malformedCounter = malformedCounter;
        }

        public List<String> getHeader() {
            return header;
        }

        public Stream<Map<String, String>> getRecordStream() {
            return recordStream;
        }

        public MalformedCounter getMalformedCounter() {
            return malformedCounter;
        }
    }
}
