package com.project.csvanalyser.aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable, comparable key for grouping. Order of values matches the group-by column order for deterministic comparison.
 */
public final class GroupKey implements Comparable<GroupKey> {

    private final List<String> values;

    public GroupKey(List<String> values) {
        this.values = values == null ? List.of() : List.copyOf(values);
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public int compareTo(GroupKey o) {
        if (this == o) return 0;
        int n = Math.min(values.size(), o.values.size());
        for (int i = 0; i < n; i++) {
            int c = values.get(i).compareTo(o.values.get(i));
            if (c != 0) return c;
        }
        return Integer.compare(values.size(), o.values.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return values.equals(groupKey.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
