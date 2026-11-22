package com.example.anonymizer.common.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/** Small helpers over the Hadoop FileSystem API (HDFS, S3A, GCS, local). */
public final class HadoopFiles {
    private HadoopFiles() {
    }

    /**
     * Writes to a hidden temp file in the same directory, then renames it into place. Atomic on HDFS
     * and local file systems. On object stores the rename is a copy, so readers must rely on manifests.
     */
    public static void writeAtomically(FileSystem fs, Path target, byte[] content) throws IOException {
        Path tmp = new Path(target.getParent(), "." + target.getName() + "." + UUID.randomUUID() + ".tmp");
        try (FSDataOutputStream out = fs.create(tmp, false)) {
            out.write(content);
        }
        if (fs.exists(target) && !fs.delete(target, false)) {
            fs.delete(tmp, false);
            throw new IOException("Could not replace " + target);
        }
        if (!fs.rename(tmp, target)) {
            fs.delete(tmp, false);
            throw new IOException("Could not rename " + tmp + " to " + target);
        }
    }

    public static Optional<byte[]> readIfExists(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            return Optional.empty();
        }
        try (FSDataInputStream in = fs.open(path)) {
            return Optional.of(in.readAllBytes());
        }
    }
}
