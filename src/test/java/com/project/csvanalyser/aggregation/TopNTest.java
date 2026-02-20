package com.project.csvanalyser.aggregation;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TopNTest {

    @Test
    void top2BySumAmount() {
        AggregationState a = new AggregationState();
        a.addRow(Map.of("amount", "100"), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")));
        a.addRow(Map.of("amount", "200"), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")));
        AggregationState b = new AggregationState();
        b.addRow(Map.of("amount", "50"), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")));
        AggregationState c = new AggregationState();
        c.addRow(Map.of("amount", "500"), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")));

        Map<GroupKey, AggregationState> state = Map.of(
                new GroupKey(List.of("A")), a,
                new GroupKey(List.of("B")), b,
                new GroupKey(List.of("C")), c
        );
        List<TopN.TopNEntry> top = TopN.compute(state, "sum_amount", 2);
        assertEquals(2, top.size());
        assertEquals("C", top.get(0).getGroupKey().getValues().get(0));
        assertEquals(500.0, top.get(0).getMetricValue());
        assertEquals("A", top.get(1).getGroupKey().getValues().get(0));
        assertEquals(300.0, top.get(1).getMetricValue());
    }

    @Test
    void top5ByCountFewerThanN() {
        AggregationState x = new AggregationState();
        x.addRow(Map.of(), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null)));
        x.addRow(Map.of(), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null)));
        AggregationState y = new AggregationState();
        y.addRow(Map.of(), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null)));

        Map<GroupKey, AggregationState> state = Map.of(
                new GroupKey(List.of("X")), x,
                new GroupKey(List.of("Y")), y
        );
        List<TopN.TopNEntry> top = TopN.compute(state, "count", 5);
        assertEquals(2, top.size());
        assertEquals(2, (int) top.get(0).getMetricValue());
        assertEquals(1, (int) top.get(1).getMetricValue());
    }

    @Test
    void tieBreakDeterministicByGroupKey() {
        AggregationState a = new AggregationState();
        a.addRow(Map.of("amount", "100"), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")));
        AggregationState b = new AggregationState();
        b.addRow(Map.of("amount", "100"), List.of(new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")));

        Map<GroupKey, AggregationState> state = Map.of(
                new GroupKey(List.of("B")), b,
                new GroupKey(List.of("A")), a
        );
        List<TopN.TopNEntry> top = TopN.compute(state, "sum_amount", 2);
        assertEquals(2, top.size());
        assertEquals(100.0, top.get(0).getMetricValue());
        assertEquals(100.0, top.get(1).getMetricValue());
        assertTrue(top.get(0).getGroupKey().compareTo(top.get(1).getGroupKey()) < 0);
    }
}
