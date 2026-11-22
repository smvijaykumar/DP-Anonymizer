package com.example.anonymizer.profile.hierarchy;

import com.typesafe.config.Config;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.AttributeType.Hierarchy.DefaultHierarchy;

import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;

/**
 * Generalization hierarchy for one quasi-identifier, built from configuration.
 *
 * <p>{@link #leaf(Object)} turns a raw value into the most specific value the hierarchy knows (for
 * example a date of birth into a birth year). {@link #generalizations(String)} returns that leaf and
 * every coarser level up to "*". Missing or unparseable values become the leaf "*".
 */
public abstract class HierarchySpec {
    public static final String MISSING = "*";

    public static HierarchySpec fromConfig(String attribute, Config spec) {
        String type = spec.getString("type");
        switch (type) {
            case "birth-year-bands":
                return new BirthYearBands(spec.getIntList("widths"));
            case "zip-mask":
                return new ZipMask(spec.getIntList("keep-digits"));
            case "category-map":
                return new CategoryMap(ResourceLoader.loadCategoryMap(spec.getString("file")),
                        spec.hasPath("unmapped-category") ? spec.getString("unmapped-category") : "OTHER");
            case "value-star":
                return new ValueStar();
            default:
                throw new IllegalArgumentException("unknown hierarchy type '" + type + "' for " + attribute);
        }
    }

    /** Most specific value for a raw input, or {@link #MISSING}. */
    public abstract String leaf(Object raw);

    /** Total number of levels, including the leaf and "*". */
    public abstract int levels();

    /** Row of the hierarchy for a leaf: [leaf, level1, ..., "*"]. */
    public final String[] generalizations(String leaf) {
        if (MISSING.equals(leaf)) {
            String[] row = new String[levels()];
            Arrays.fill(row, MISSING);
            return row;
        }
        return generalizeKnown(leaf);
    }

    protected abstract String[] generalizeKnown(String leaf);

    /** ARX hierarchy covering exactly the given leaves. */
    public Hierarchy build(Collection<String> leaves) {
        DefaultHierarchy h = Hierarchy.create();
        for (String leaf : new TreeSet<>(leaves)) {
            h.add(generalizations(leaf));
        }
        return h;
    }
}
