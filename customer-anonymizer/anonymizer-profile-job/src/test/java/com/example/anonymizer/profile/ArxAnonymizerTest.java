package com.example.anonymizer.profile;

import com.example.anonymizer.profile.arx.ArxAnonymizer;
import com.example.anonymizer.profile.hierarchy.HierarchySpec;
import com.typesafe.config.ConfigFactory;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ArxAnonymizerTest {
    private static final String[] ZIPS = {"33326", "33327", "33331", "10001", "10002", "94105"};
    private static final String[] INCOME = {"LOW", "MID", "HIGH", "VERY_HIGH"};

    @Test
    void outputIsKAnonymousLDiverseAndRowAligned() throws Exception {
        HierarchySpec dob = HierarchySpec.fromConfig("dob", ConfigFactory.parseString("type = birth-year-bands, widths = [5, 10, 20]"));
        HierarchySpec zip = HierarchySpec.fromConfig("zip", ConfigFactory.parseString("type = zip-mask, keep-digits = [5, 3]"));
        HierarchySpec gender = HierarchySpec.fromConfig("gender", ConfigFactory.parseString("type = value-star"));

        Random rnd = new Random(7);
        List<String[]> rows = new ArrayList<>();
        Map<String, Set<String>> leaves = Map.of("dob", new HashSet<>(), "zip", new HashSet<>(), "gender", new HashSet<>());
        for (int i = 0; i < 300; i++) {
            String[] r = {
                    dob.leaf(Integer.toString(1950 + rnd.nextInt(50))),
                    zip.leaf(ZIPS[rnd.nextInt(ZIPS.length)]),
                    gender.leaf(rnd.nextBoolean() ? "M" : "F"),
                    INCOME[rnd.nextInt(INCOME.length)]};
            leaves.get("dob").add(r[0]);
            leaves.get("zip").add(r[1]);
            leaves.get("gender").add(r[2]);
            rows.add(r);
        }
        Map<String, Hierarchy> hierarchies = new LinkedHashMap<>();
        hierarchies.put("dob", dob.build(leaves.get("dob")));
        hierarchies.put("zip", zip.build(leaves.get("zip")));
        hierarchies.put("gender", gender.build(leaves.get("gender")));

        ArxAnonymizer.Outcome o = new ArxAnonymizer().anonymize(List.of("dob", "zip", "gender"), List.of("incomeBand"),
                rows, hierarchies, new ArxAnonymizer.Params(5, 2, 0.05), null);

        Map<String, Integer> classSizes = new HashMap<>();
        Map<String, Set<String>> classSensitive = new HashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            String[] out = o.rows().get(i);
            assertThat(out[3]).as("sensitive value stays aligned with its input row").isEqualTo(rows.get(i)[3]);
            if (o.suppressed()[i]) {
                continue;
            }
            String key = out[0] + "|" + out[1] + "|" + out[2];
            classSizes.merge(key, 1, Integer::sum);
            classSensitive.computeIfAbsent(key, k -> new HashSet<>()).add(out[3]);
        }
        assertThat(classSizes.values()).allSatisfy(size -> assertThat(size).isGreaterThanOrEqualTo(5));
        assertThat(classSensitive.values()).allSatisfy(s -> assertThat(s.size()).isGreaterThanOrEqualTo(2));
        assertThat(o.suppressedCount()).isLessThanOrEqualTo(15);
        assertThat(o.highestRisk()).isLessThanOrEqualTo(0.2 + 1e-9);
        assertThat(o.levels()).containsOnlyKeys("dob", "zip", "gender");
    }

    @Test
    void pinnedLevelsAreReusedWhenStillValid() throws Exception {
        HierarchySpec gender = HierarchySpec.fromConfig("gender", ConfigFactory.parseString("type = value-star"));
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            rows.add(new String[] {i % 2 == 0 ? "M" : "F", i % 3 == 0 ? "LOW" : (i % 3 == 1 ? "MID" : "HIGH")});
        }
        Map<String, Hierarchy> h = Map.of("gender", gender.build(Set.of("M", "F")));
        ArxAnonymizer.Outcome o = new ArxAnonymizer().anonymize(List.of("gender"), List.of("incomeBand"), rows, h,
                new ArxAnonymizer.Params(5, 2, 0.0), Map.of("gender", 1));
        assertThat(o.usedPinnedLevels()).isTrue();
        assertThat(o.levels()).containsEntry("gender", 1);
    }
}
