package com.project.csvanalyser.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Recursive-descent parser for filter expressions: column op value, combined with && and ||.
 */
final class FilterExpressionParser {

    private final String input;
    private final Set<String> validColumns;
    private int pos;

    FilterExpressionParser(String input, Set<String> validColumns) {
        this.input = input;
        this.validColumns = validColumns;
        this.pos = 0;
    }

    RowPredicate parse() {
        RowPredicate p = parseOr();
        if (pos < input.length()) {
            throw new IllegalArgumentException("Unexpected character at position " + pos + ": '" + input.substring(pos) + "'");
        }
        return p;
    }

    private RowPredicate parseOr() {
        List<RowPredicate> terms = new ArrayList<>();
        terms.add(parseAnd());
        while (pos < input.length()) {
            skipWhitespace();
            if (consume("||")) {
                skipWhitespace();
                terms.add(parseAnd());
            } else {
                break;
            }
        }
        return row -> terms.stream().anyMatch(p -> p.test(row));
    }

    private RowPredicate parseAnd() {
        List<RowPredicate> terms = new ArrayList<>();
        terms.add(parsePrimary());
        while (pos < input.length()) {
            skipWhitespace();
            if (consume("&&")) {
                skipWhitespace();
                terms.add(parsePrimary());
            } else {
                break;
            }
        }
        return row -> terms.stream().allMatch(p -> p.test(row));
    }

    private RowPredicate parsePrimary() {
        skipWhitespace();
        if (pos >= input.length()) {
            throw new IllegalArgumentException("Unexpected end of expression");
        }
        RowPredicate cond = parseCondition();
        skipWhitespace();
        return cond;
    }

    private RowPredicate parseCondition() {
        String column = parseIdentifier();
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("Expected column name at position " + pos);
        }
        if (!validColumns.contains(column)) {
            throw new IllegalArgumentException("Unknown column in filter: '" + column + "'. Available columns: " + validColumns);
        }
        skipWhitespace();
        String op = parseOperator();
        skipWhitespace();
        Object value = parseValue();
        return row -> {
            String cell = row.getOrDefault(column, "");
            return evaluate(op, column, cell, value);
        };
    }

    private String parseIdentifier() {
        int start = pos;
        while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
            pos++;
        }
        return start < pos ? input.substring(start, pos) : "";
    }

    private String parseOperator() {
        if (consume("==")) return "==";
        if (consume("!=")) return "!=";
        if (consume(">=")) return ">=";
        if (consume("<=")) return "<=";
        if (consume(">")) return ">";
        if (consume("<")) return "<";
        throw new IllegalArgumentException("Expected operator (==, !=, >, <, >=, <=) at position " + pos);
    }

    private Object parseValue() {
        if (pos >= input.length()) {
            throw new IllegalArgumentException("Expected value at position " + pos);
        }
        if (input.charAt(pos) == '"') {
            pos++;
            int start = pos;
            while (pos < input.length() && input.charAt(pos) != '"') {
                if (input.charAt(pos) == '\\') pos++;
                pos++;
            }
            if (pos >= input.length()) {
                throw new IllegalArgumentException("Unclosed string literal");
            }
            String s = input.substring(start, pos).replace("\\\"", "\"");
            pos++;
            return s;
        }
        int start = pos;
        boolean dot = false;
        while (pos < input.length()) {
            char c = input.charAt(pos);
            if (Character.isDigit(c) || c == '.' || (c == '-' && pos == start)) {
                if (c == '.') dot = true;
                pos++;
            } else {
                break;
            }
        }
        String num = input.substring(start, pos).trim();
        if (num.isEmpty()) {
            throw new IllegalArgumentException("Expected value (string or number) at position " + start);
        }
        try {
            if (dot || num.contains(".")) {
                return Double.parseDouble(num);
            }
            return Long.parseLong(num);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number: " + num, e);
        }
    }

    private boolean evaluate(String op, String column, String cellValue, Object filterValue) {
        if (filterValue instanceof String) {
            int cmp = cellValue.compareTo((String) filterValue);
            return switch (op) {
                case "==" -> cmp == 0;
                case "!=" -> cmp != 0;
                case ">", ">=", "<", "<=" -> throw new IllegalArgumentException("String comparison only supports == and != for column " + column);
                default -> false;
            };
        }
        double cellNum;
        try {
            cellNum = cellValue.isBlank() ? 0 : Double.parseDouble(cellValue.trim());
        } catch (NumberFormatException e) {
            return false;
        }
        double num = ((Number) filterValue).doubleValue();
        return switch (op) {
            case "==" -> cellNum == num;
            case "!=" -> cellNum != num;
            case ">" -> cellNum > num;
            case "<" -> cellNum < num;
            case ">=" -> cellNum >= num;
            case "<=" -> cellNum <= num;
            default -> false;
        };
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }

    private boolean consume(String s) {
        if (input.regionMatches(pos, s, 0, s.length())) {
            pos += s.length();
            return true;
        }
        return false;
    }
}
