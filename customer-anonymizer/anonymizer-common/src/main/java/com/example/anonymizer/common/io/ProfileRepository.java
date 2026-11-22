package com.example.anonymizer.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Versioned profile table layout:
 * <pre>
 *   root/version=20261112T020000Z/part-*.parquet
 *   root/version=20261112T020000Z/_manifest.json
 *   root/_LATEST                      (text: the current version id)
 * </pre>
 * Versions are immutable. Publishing moves a fully written staging directory into place, writes its
 * manifest, and only then advances {@code _LATEST}.
 */
public final class ProfileRepository {
    public static final String LATEST = "_LATEST";
    public static final String MANIFEST = "_manifest.json";

    private final Path root;
    private final FileSystem fs;

    public ProfileRepository(Path root, Configuration conf) throws IOException {
        this.root = root;
        this.fs = root.getFileSystem(conf);
    }

    public Path versionPath(String version) {
        return new Path(root, "version=" + version);
    }

    public Path stagingPath(String version) {
        return new Path(root, "_staging/" + version);
    }

    public Optional<String> latestVersion() throws IOException {
        return HadoopFiles.readIfExists(fs, new Path(root, LATEST))
                .map(b -> new String(b, StandardCharsets.UTF_8).trim())
                .filter(s -> !s.isEmpty());
    }

    public Optional<ProfileManifest> manifest(String version) throws IOException {
        Optional<byte[]> bytes = HadoopFiles.readIfExists(fs, new Path(versionPath(version), MANIFEST));
        if (bytes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Json.MAPPER.readValue(bytes.get(), ProfileManifest.class));
    }

    public void publish(Path stagingDir, ProfileManifest manifest) throws IOException {
        Path target = versionPath(manifest.version());
        if (fs.exists(target)) {
            throw new IOException("Profile version already exists: " + target);
        }
        fs.mkdirs(root);
        if (!fs.rename(stagingDir, target)) {
            throw new IOException("Could not move " + stagingDir + " to " + target);
        }
        HadoopFiles.writeAtomically(fs, new Path(target, MANIFEST), Json.bytes(manifest));
        HadoopFiles.writeAtomically(fs, new Path(root, LATEST), manifest.version().getBytes(StandardCharsets.UTF_8));
    }
}
