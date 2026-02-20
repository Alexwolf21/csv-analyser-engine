package com.project.csvanalyser.filter;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FilterParserTest {

    private static final List<String> HEADER = List.of("region", "amount", "product");

    @Test
    void stringEquality() {
        RowPredicate p = FilterParser.parse("region==\"APAC\"", HEADER);
        assertTrue(p.test(Map.of("region", "APAC", "amount", "100", "product", "X")));
        assertFalse(p.test(Map.of("region", "EMEA", "amount", "100", "product", "X")));
    }

    @Test
    void stringInequality() {
        RowPredicate p = FilterParser.parse("region!=\"EMEA\"", HEADER);
        assertTrue(p.test(Map.of("region", "APAC", "amount", "100")));
        assertFalse(p.test(Map.of("region", "EMEA", "amount", "100")));
    }

    @Test
    void numericComparisons() {
        RowPredicate p = FilterParser.parse("amount>1000", HEADER);
        assertTrue(p.test(Map.of("region", "X", "amount", "1500", "product", "Y")));
        assertFalse(p.test(Map.of("region", "X", "amount", "500", "product", "Y")));
        assertFalse(p.test(Map.of("region", "X", "amount", "1000", "product", "Y")));

        p = FilterParser.parse("amount<=500", HEADER);
        assertTrue(p.test(Map.of("amount", "500")));
        assertTrue(p.test(Map.of("amount", "100")));
        assertFalse(p.test(Map.of("amount", "501")));
    }

    @Test
    void combinedAnd() {
        RowPredicate p = FilterParser.parse("region==\"APAC\" && amount>1000", HEADER);
        assertTrue(p.test(Map.of("region", "APAC", "amount", "1200")));
        assertFalse(p.test(Map.of("region", "APAC", "amount", "500")));
        assertFalse(p.test(Map.of("region", "EMEA", "amount", "1200")));
    }

    @Test
    void combinedOr() {
        RowPredicate p = FilterParser.parse("region==\"APAC\" || region==\"EMEA\"", HEADER);
        assertTrue(p.test(Map.of("region", "APAC")));
        assertTrue(p.test(Map.of("region", "EMEA")));
        assertFalse(p.test(Map.of("region", "LATAM")));
    }

    @Test
    void missingColumnFailsFast() {
        assertThrows(IllegalArgumentException.class, () ->
                FilterParser.parse("unknown==\"x\"", HEADER));
        assertThrows(IllegalArgumentException.class, () ->
                FilterParser.parse("region==\"APAC\" && foo>1", HEADER));
    }

    @Test
    void nullOrBlankExpressionAlwaysTrue() {
        assertTrue(FilterParser.parse(null, HEADER).test(Map.of("region", "X")));
        assertTrue(FilterParser.parse("", HEADER).test(Map.of("region", "X")));
        assertTrue(FilterParser.parse("   ", HEADER).test(Map.of("region", "X")));
    }
}
