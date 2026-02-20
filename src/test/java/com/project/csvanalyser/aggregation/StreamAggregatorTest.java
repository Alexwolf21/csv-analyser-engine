package com.project.csvanalyser.aggregation;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class StreamAggregatorTest {

    @Test
    void groupByOneColumnCountAndSum() {
        List<AggregationSpec.AggregationOpWithColumn> aggs = List.of(
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null),
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")
        );
        AggregationSpec spec = new AggregationSpec(List.of("product"), aggs);
        StreamAggregator agg = new StreamAggregator(spec);
        agg.consume(Stream.of(
                Map.of("product", "A", "amount", "100"),
                Map.of("product", "A", "amount", "200"),
                Map.of("product", "B", "amount", "50")
        ));
        Map<GroupKey, AggregationState> state = agg.getStateByGroup();
        assertEquals(2, state.size());
        GroupKey keyA = new GroupKey(List.of("A"));
        GroupKey keyB = new GroupKey(List.of("B"));
        assertEquals(2, state.get(keyA).getCount());
        assertEquals(300.0, state.get(keyA).getSum("amount"));
        assertEquals(150.0, state.get(keyA).getAvg("amount"));
        assertEquals(1, state.get(keyB).getCount());
        assertEquals(50.0, state.get(keyB).getSum("amount"));
        // Deterministic order: keys sorted
        List<GroupKey> keys = List.copyOf(state.keySet());
        assertTrue(keys.get(0).compareTo(keys.get(1)) <= 0);
    }

    @Test
    void groupByTwoColumns() {
        List<AggregationSpec.AggregationOpWithColumn> aggs = List.of(
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null),
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount")
        );
        AggregationSpec spec = new AggregationSpec(List.of("product", "region"), aggs);
        StreamAggregator agg = new StreamAggregator(spec);
        agg.consume(Stream.of(
                Map.of("product", "A", "region", "APAC", "amount", "100"),
                Map.of("product", "A", "region", "APAC", "amount", "200"),
                Map.of("product", "A", "region", "EMEA", "amount", "50")
        ));
        Map<GroupKey, AggregationState> state = agg.getStateByGroup();
        assertEquals(2, state.size());
        GroupKey keyApac = new GroupKey(List.of("A", "APAC"));
        GroupKey keyEmea = new GroupKey(List.of("A", "EMEA"));
        assertEquals(2, state.get(keyApac).getCount());
        assertEquals(300.0, state.get(keyApac).getSum("amount"));
        assertEquals(1, state.get(keyEmea).getCount());
        assertEquals(50.0, state.get(keyEmea).getSum("amount"));
    }

    @Test
    void noGroupByGlobalAggregate() {
        List<AggregationSpec.AggregationOpWithColumn> aggs = List.of(
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null),
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.SUM, "amount"),
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.MIN, "amount"),
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.MAX, "amount")
        );
        AggregationSpec spec = new AggregationSpec(List.of(), aggs);
        StreamAggregator agg = new StreamAggregator(spec);
        agg.consume(Stream.of(
                Map.of("amount", "10"),
                Map.of("amount", "20"),
                Map.of("amount", "30")
        ));
        Map<GroupKey, AggregationState> state = agg.getStateByGroup();
        assertEquals(1, state.size());
        GroupKey global = new GroupKey(List.of());
        AggregationState s = state.get(global);
        assertEquals(3, s.getCount());
        assertEquals(60.0, s.getSum("amount"));
        assertEquals(10.0, s.getMin("amount"));
        assertEquals(30.0, s.getMax("amount"));
        assertEquals(20.0, s.getAvg("amount"));
    }

    @Test
    void deterministicKeyOrder() {
        List<AggregationSpec.AggregationOpWithColumn> aggs = List.of(
                new AggregationSpec.AggregationOpWithColumn(AggregationSpec.AggregationOp.COUNT, null)
        );
        AggregationSpec spec = new AggregationSpec(List.of("x"), aggs);
        StreamAggregator agg = new StreamAggregator(spec);
        agg.consume(Stream.of(
                Map.of("x", "b"),
                Map.of("x", "a"),
                Map.of("x", "c")
        ));
        List<GroupKey> keys = List.copyOf(agg.getStateByGroup().keySet());
        assertEquals(3, keys.size());
        assertEquals("a", keys.get(0).getValues().get(0));
        assertEquals("b", keys.get(1).getValues().get(0));
        assertEquals("c", keys.get(2).getValues().get(0));
    }
}
