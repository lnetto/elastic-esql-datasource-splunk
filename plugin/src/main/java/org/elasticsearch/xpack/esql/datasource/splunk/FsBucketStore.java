/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Buckets on a filesystem visible to every node — typically a frozen archive
 * ({@code coldToFrozenDir}) on NFS, or a SmartStore bucket tree synced down from S3.
 *
 * <p>The ES entitlement policy only grants reads below {@code path.repo}, so the root must
 * live under one of the node's {@code path.repo} directories.
 */
final class FsBucketStore implements BucketStore {

    private final Path root;

    FsBucketStore(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    @Override
    public void list(Consumer<Entry> sink) throws IOException {
        if (Files.isDirectory(root) == false) {
            throw new IOException("bucket root [" + root + "] is not a directory");
        }
        try (Stream<Path> paths = Files.find(root, 32, (p, attrs) -> attrs.isRegularFile())) {
            paths.forEach(p -> {
                String key = root.relativize(p).toString().replace('\\', '/');
                if (BucketStore.interesting(key)) {
                    sink.accept(new Entry(key, size(p)));
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static long size(Path p) {
        try {
            return Files.readAttributes(p, BasicFileAttributes.class).size();
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public InputStream open(String key) throws IOException {
        Path p = root.resolve(key).normalize();
        if (p.startsWith(root) == false) {
            throw new IOException("key [" + key + "] escapes bucket root [" + root + "]");
        }
        return Files.newInputStream(p);
    }

    @Override
    public String describe() {
        return root.toString();
    }

    @Override
    public void close() {}
}
