package com.example.anonymizer.stream;

import com.example.anonymizer.common.io.ProfileManifest;
import com.example.anonymizer.common.io.ProfileRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static net.logstash.logback.argument.StructuredArguments.kv;

/** Keeps the latest profile version cached and swaps it when the profile job publishes a new one. */
final class ProfileCache {
    private static final Logger LOG = LoggerFactory.getLogger(ProfileCache.class);

    record Loaded(ProfileManifest manifest, Dataset<Row> data) {
    }

    private final ProfileRepository repo;
    private final String expectedSchemaVersion;
    private Loaded current;

    ProfileCache(ProfileRepository repo, String expectedSchemaVersion) {
        this.repo = repo;
        this.expectedSchemaVersion = expectedSchemaVersion;
    }

    Loaded latest(SparkSession spark) throws IOException {
        String version = repo.latestVersion().orElseThrow(() ->
                new IllegalStateException("No published profile version; run ProfileAnonymizationJob first"));
        if (current != null && current.manifest().version().equals(version)) {
            return current;
        }
        ProfileManifest manifest = repo.manifest(version).orElseThrow(() ->
                new IllegalStateException("Profile version " + version + " has no manifest"));
        if (!manifest.schemaVersion().equals(expectedSchemaVersion)) {
            throw new IllegalStateException("Profile version " + version + " was built for schema "
                    + manifest.schemaVersion() + " but the stream job runs schema " + expectedSchemaVersion);
        }
        Dataset<Row> data = spark.read().parquet(repo.versionPath(version).toString())
                .persist(StorageLevel.MEMORY_AND_DISK());
        if (current != null) {
            current.data().unpersist();
        }
        current = new Loaded(manifest, data);
        LOG.info("Profile version loaded", kv("profileVersion", version), kv("records", manifest.recordCount()));
        return current;
    }
}
