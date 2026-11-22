package com.example.anonymizer.profile.hierarchy;

import java.util.List;

/** Postal code masking, e.g. keep-digits [5,3]: 33326 to 333** to *. */
final class ZipMask extends HierarchySpec {
    private final int[] keep;

    ZipMask(List<Integer> keepDigits) {
        if (keepDigits.isEmpty()) {
            throw new IllegalArgumentException("zip-mask needs keep-digits");
        }
        this.keep = keepDigits.stream().mapToInt(Integer::intValue).toArray();
        for (int i = 0; i < keep.length; i++) {
            if (keep[i] < 1 || keep[i] > 10 || (i > 0 && keep[i] >= keep[i - 1])) {
                throw new IllegalArgumentException("keep-digits must be strictly decreasing values between 1 and 10: " + keepDigits);
            }
        }
    }

    @Override
    public String leaf(Object raw) {
        if (raw == null) {
            return MISSING;
        }
        String digits = raw.toString().replaceAll("[^0-9]", "");
        return digits.length() < keep[0] ? MISSING : digits.substring(0, keep[0]);
    }

    @Override
    public int levels() {
        return keep.length + 1;
    }

    @Override
    protected String[] generalizeKnown(String leaf) {
        String[] row = new String[levels()];
        for (int i = 0; i < keep.length; i++) {
            row[i] = leaf.substring(0, keep[i]) + "*".repeat(keep[0] - keep[i]);
        }
        row[row.length - 1] = MISSING;
        return row;
    }
}
