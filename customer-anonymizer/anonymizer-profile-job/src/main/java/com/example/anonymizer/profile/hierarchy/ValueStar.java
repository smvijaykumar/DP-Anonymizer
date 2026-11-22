package com.example.anonymizer.profile.hierarchy;

import java.util.Locale;

/** Value to * (for low-cardinality attributes such as gender). */
final class ValueStar extends HierarchySpec {
    @Override
    public String leaf(Object raw) {
        if (raw == null) {
            return MISSING;
        }
        String v = raw.toString().trim().toUpperCase(Locale.ROOT);
        return v.isEmpty() ? MISSING : v;
    }

    @Override
    public int levels() {
        return 2;
    }

    @Override
    protected String[] generalizeKnown(String leaf) {
        return new String[] {leaf, MISSING};
    }
}
