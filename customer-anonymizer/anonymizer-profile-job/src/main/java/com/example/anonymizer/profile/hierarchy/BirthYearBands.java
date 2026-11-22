package com.example.anonymizer.profile.hierarchy;

import java.sql.Date;
import java.time.LocalDate;
import java.time.Year;
import java.util.List;

/**
 * Date of birth to birth year, then to aligned bands (e.g. widths [5,10,20]):
 * 1984 to 1980-1984 to 1980-1989 to 1980-1999 to *. Each width must divide the next so bands nest.
 * Birth-year bands, unlike age bands, do not drift from day to day, so values stay stable.
 */
final class BirthYearBands extends HierarchySpec {
    private static final int MIN_YEAR = 1900;
    private final int[] widths;

    BirthYearBands(List<Integer> widths) {
        if (widths.isEmpty()) {
            throw new IllegalArgumentException("birth-year-bands needs at least one width");
        }
        this.widths = widths.stream().mapToInt(Integer::intValue).toArray();
        for (int i = 0; i < this.widths.length; i++) {
            if (this.widths[i] <= 1) {
                throw new IllegalArgumentException("band widths must be greater than 1");
            }
            if (i > 0 && (this.widths[i] <= this.widths[i - 1] || this.widths[i] % this.widths[i - 1] != 0)) {
                throw new IllegalArgumentException("each band width must be larger than and a multiple of the previous: " + widths);
            }
        }
    }

    @Override
    public String leaf(Object raw) {
        Integer year = yearOf(raw);
        if (year == null || year < MIN_YEAR || year > Year.now().getValue() + 1) {
            return MISSING;
        }
        return Integer.toString(year);
    }

    private static Integer yearOf(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof Date) {
            return ((Date) raw).toLocalDate().getYear();
        }
        if (raw instanceof LocalDate) {
            return ((LocalDate) raw).getYear();
        }
        if (raw instanceof Number) {
            return ((Number) raw).intValue();
        }
        String s = raw.toString().trim();
        try {
            if (s.matches("\\d{4}")) {
                return Integer.parseInt(s);
            }
            if (s.length() >= 10) {
                return LocalDate.parse(s.substring(0, 10)).getYear();
            }
        } catch (RuntimeException e) {
            return null;
        }
        return null;
    }

    @Override
    public int levels() {
        return widths.length + 2;
    }

    @Override
    protected String[] generalizeKnown(String leaf) {
        int year = Integer.parseInt(leaf);
        String[] row = new String[levels()];
        row[0] = leaf;
        for (int i = 0; i < widths.length; i++) {
            int start = year - Math.floorMod(year, widths[i]);
            row[i + 1] = start + "-" + (start + widths[i] - 1);
        }
        row[row.length - 1] = MISSING;
        return row;
    }
}
