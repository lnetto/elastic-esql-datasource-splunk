/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Minimal {@link StorageProvider} for {@code splunkfs://} and {@code splunks3://}. Buckets are
 * read through the {@link SplunkConnector}, not as byte streams; this exists so the resolver can
 * register a FileList entry for the schemes (same role FlightStorageProvider plays for Flight).
 */
public final class SplunkStorageProvider implements StorageProvider {

    @Override
    public StorageObject newObject(StoragePath path) {
        return newObject(path, 0L, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return newObject(path, length, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new BucketTreeObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
        throw new UnsupportedOperationException("Splunk bucket trees are listed by the splunk connector");
    }

    @Override
    public boolean exists(StoragePath path) {
        validateScheme(path);
        // Real existence is checked by the connector's listing at schema-resolution time.
        return true;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of(SplunkConfig.FS_SCHEME, SplunkConfig.S3_SCHEME);
    }

    @Override
    public boolean supportsStableMetadata() {
        // A bucket tree changes as Splunk freezes/uploads buckets; don't cache under a stale identity.
        return false;
    }

    @Override
    public void close() {}

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (supported(scheme) == false) {
            throw new IllegalArgumentException("SplunkStorageProvider only supports splunkfs:// and splunks3://, got: " + scheme);
        }
    }

    private static boolean supported(String scheme) {
        return SplunkConfig.FS_SCHEME.equals(scheme) || SplunkConfig.S3_SCHEME.equals(scheme);
    }

    private record BucketTreeObject(StoragePath path, long knownLength, Instant knownLastModified) implements StorageObject {

        @Override
        public InputStream newStream() throws IOException {
            throw notByteAddressable();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            throw notByteAddressable();
        }

        private static IOException notByteAddressable() {
            return new IOException("Splunk buckets are read via the splunk connector, not as byte streams");
        }

        @Override
        public long length() {
            return knownLength;
        }

        @Override
        public Instant lastModified() {
            return knownLastModified;
        }

        @Override
        public boolean exists() {
            return true;
        }
    }
}
