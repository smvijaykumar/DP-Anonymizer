package com.example.anonymizer.profile.hierarchy;

import java.util.Locale;
import java.util.Map;

/** Value to category (from a mapping file) to *. Unmapped values go to a configurable category. */
final class CategoryMap extends HierarchySpec {
    private final Map<String, String> categories;
    private final String unmapped;

    CategoryMap(Map<String, String> categories, String unmapped) {
        this.categories = categories;
        this.unmapped = unmapped;
    }

    @Override
    public String leaf(Object raw) {
        if (raw == null) {
            return MISSING;
        }
        String v = raw.toString().trim().toLowerCase(Locale.ROOT);
        return v.isEmpty() ? MISSING : v;
    }

    @Override
    public int levels() {
        return 3;
    }

    @Override
    protected String[] generalizeKnown(String leaf) {
        return new String[] {leaf, categories.getOrDefault(leaf, unmapped), MISSING};
    }
}
