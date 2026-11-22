package com.example.anonymizer.profile.hierarchy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Loads "value,category" mapping files from the file system or from "classpath:" locations. */
final class ResourceLoader {
    private ResourceLoader() {
    }

    static Map<String, String> loadCategoryMap(String location) {
        String text = read(location);
        Map<String, String> map = new HashMap<>();
        int lineNo = 0;
        for (String line : text.split("\\R")) {
            lineNo++;
            String l = line.strip();
            if (l.isEmpty() || l.startsWith("#") || (lineNo == 1 && l.equalsIgnoreCase("value,category"))) {
                continue;
            }
            int comma = l.indexOf(',');
            if (comma <= 0 || comma == l.length() - 1) {
                throw new IllegalArgumentException("Malformed line " + lineNo + " in category map " + location);
            }
            map.put(l.substring(0, comma).strip().toLowerCase(Locale.ROOT), l.substring(comma + 1).strip());
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException("Category map " + location + " is empty");
        }
        return map;
    }

    private static String read(String location) {
        try {
            if (location.startsWith("classpath:")) {
                String res = location.substring("classpath:".length());
                try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(res)) {
                    if (in == null) {
                        throw new IllegalArgumentException("Classpath resource not found: " + res);
                    }
                    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
            return Files.readString(Path.of(location));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + location, e);
        }
    }
}
