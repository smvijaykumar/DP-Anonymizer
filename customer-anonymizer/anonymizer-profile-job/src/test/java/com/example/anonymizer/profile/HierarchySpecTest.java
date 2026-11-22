package com.example.anonymizer.profile;

import com.example.anonymizer.profile.hierarchy.HierarchySpec;
import com.typesafe.config.ConfigFactory;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HierarchySpecTest {
    private static HierarchySpec spec(String hocon) {
        return HierarchySpec.fromConfig("attr", ConfigFactory.parseString(hocon));
    }

    @Test
    void birthYearBandsGeneralizeAndNest() {
        HierarchySpec s = spec("type = birth-year-bands, widths = [5, 10, 20]");
        assertThat(s.leaf("1984-03-12")).isEqualTo("1984");
        assertThat(s.leaf(Date.valueOf(LocalDate.of(1984, 3, 12)))).isEqualTo("1984");
        assertThat(s.generalizations("1984")).containsExactly("1984", "1980-1984", "1980-1989", "1980-1999", "*");
        for (int year = 1900; year <= 2025; year++) {
            String[] row = s.generalizations(Integer.toString(year));
            for (int lvl = 1; lvl < row.length - 2; lvl++) {
                int[] inner = bounds(row[lvl]);
                int[] outer = bounds(row[lvl + 1]);
                assertThat(inner[0]).isGreaterThanOrEqualTo(outer[0]);
                assertThat(inner[1]).isLessThanOrEqualTo(outer[1]);
            }
        }
    }

    @Test
    void birthYearBandsMapBadValuesToStar() {
        HierarchySpec s = spec("type = birth-year-bands, widths = [5, 10]");
        assertThat(s.leaf(null)).isEqualTo("*");
        assertThat(s.leaf("not a date")).isEqualTo("*");
        assertThat(s.leaf("1850-01-01")).isEqualTo("*");
        assertThat(s.generalizations("*")).containsOnly("*").hasSize(4);
    }

    @Test
    void bandWidthsMustNest() {
        assertThatThrownBy(() -> spec("type = birth-year-bands, widths = [5, 7]"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void zipMaskKeepsLeadingDigits() {
        HierarchySpec s = spec("type = zip-mask, keep-digits = [5, 3]");
        assertThat(s.leaf("33326-1234")).isEqualTo("33326");
        assertThat(s.leaf("333")).isEqualTo("*");
        assertThat(s.generalizations("33326")).containsExactly("33326", "333**", "*");
    }

    @Test
    void categoryMapUsesFileAndUnmappedCategory() {
        HierarchySpec s = spec("type = category-map, file = \"classpath:occupation-categories-test.csv\", unmapped-category = OTHER");
        assertThat(s.leaf(" Nurse ")).isEqualTo("nurse");
        assertThat(s.generalizations("nurse")).containsExactly("nurse", "HEALTHCARE", "*");
        assertThat(s.generalizations("astronaut")).containsExactly("astronaut", "OTHER", "*");
    }

    @Test
    void valueStarHasTwoLevels() {
        HierarchySpec s = spec("type = value-star");
        assertThat(s.leaf("f")).isEqualTo("F");
        assertThat(s.generalizations("F")).containsExactly("F", "*");
    }

    @Test
    void buildsArxHierarchyCoveringEveryLeaf() {
        HierarchySpec s = spec("type = zip-mask, keep-digits = [5, 3]");
        Hierarchy h = s.build(List.of("33326", "33327", "10001", "*"));
        String[][] rows = h.getHierarchy();
        assertThat(rows).hasNumberOfRows(4);
        for (String[] r : rows) {
            assertThat(r).hasSize(3);
        }
    }

    private static int[] bounds(String band) {
        String[] p = band.split("-");
        return new int[] {Integer.parseInt(p[0]), Integer.parseInt(p[1])};
    }
}
