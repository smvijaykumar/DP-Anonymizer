package com.example.anonymizer.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Moves files Spark wrote to a staging directory into a partition directory, then writes the batch
 * manifest and {@code _SUCCESS}. The manifest is the commit point: a batch whose manifest exists is
 * complete, and a replay of that batch id is skipped. Several batches may land in one partition
 * (e.g. after a restart within the same hour); each has its own manifest and file prefix.
 */
public final class AtomicPartitionCommitter {
    public static final String SUCCESS = "_SUCCESS";
    private final FileSystem fs;

    public AtomicPartitionCommitter(Path root, Configuration conf) throws IOException {
        this.fs = root.getFileSystem(conf);
    }

    public static String manifestName(long batchId) {
        return "_manifest-b" + batchId + ".json";
    }

    public boolean isCommitted(Path partitionDir, long batchId) throws IOException {
        return fs.exists(new Path(partitionDir, manifestName(batchId)));
    }

    public void deleteIfExists(Path path) throws IOException {
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public List<String> commit(Path stagingDir, Path partitionDir, long batchId,
                               Function<List<String>, byte[]> manifestFactory) throws IOException {
        fs.mkdirs(partitionDir);
        FileStatus[] staged = fs.exists(stagingDir)
                ? fs.listStatus(stagingDir, p -> !p.getName().startsWith("_") && !p.getName().startsWith("."))
                : new FileStatus[0];
        Arrays.sort(staged, Comparator.comparing(s -> s.getPath().getName()));

        List<String> committed = new ArrayList<>();
        for (FileStatus f : staged) {
            String name = f.getPath().getName();
            String target = "part-b" + batchId + "-" + (name.startsWith("part-") ? name.substring(5) : name);
            Path targetPath = new Path(partitionDir, target);
            if (fs.exists(targetPath)) {
                fs.delete(targetPath, false); // left over from an earlier failed attempt of this batch
            }
            if (!fs.rename(f.getPath(), targetPath)) {
                throw new IOException("Rename failed: " + f.getPath() + " -> " + targetPath);
            }
            committed.add(target);
        }
        HadoopFiles.writeAtomically(fs, new Path(partitionDir, manifestName(batchId)), manifestFactory.apply(committed));
        HadoopFiles.writeAtomically(fs, new Path(partitionDir, SUCCESS), new byte[0]);
        deleteIfExists(stagingDir);
        return committed;
    }
}
