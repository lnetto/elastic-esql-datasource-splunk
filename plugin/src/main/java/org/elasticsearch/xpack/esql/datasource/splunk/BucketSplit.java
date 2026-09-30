/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.io.IOException;
import java.util.Objects;

/**
 * One Splunk bucket as a unit of parallel work. Carries the exact object keys of the journal
 * (whole file or ordered slices) so data nodes never re-list the store.
 */
public final class BucketSplit implements ExternalSplit {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        ExternalSplit.class,
        "SplunkBucketSplit",
        BucketSplit::new
    );

    private final BucketDiscovery.BucketRef bucket;

    BucketSplit(BucketDiscovery.BucketRef bucket) {
        this.bucket = Objects.requireNonNull(bucket);
    }

    BucketSplit(StreamInput in) throws IOException {
        this.bucket = new BucketDiscovery.BucketRef(
            in.readString(),
            in.readString(),
            in.readString(),
            in.readStringCollectionAsImmutableList(),
            in.readLong(),
            in.readLong(),
            in.readLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(bucket.index());
        out.writeString(bucket.bucketId());
        out.writeString(bucket.dir());
        out.writeStringCollection(bucket.journalKeys());
        out.writeLong(bucket.newest());
        out.writeLong(bucket.oldest());
        out.writeLong(bucket.storedBytes());
    }

    BucketDiscovery.BucketRef bucket() {
        return bucket;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String sourceType() {
        return SplunkConfig.TYPE;
    }

    @Override
    public long estimatedSizeInBytes() {
        return bucket.storedBytes() > 0 ? bucket.storedBytes() : -1;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BucketSplit other && bucket.equals(other.bucket);
    }

    @Override
    public int hashCode() {
        return bucket.hashCode();
    }

    @Override
    public String toString() {
        return "SplunkBucketSplit[" + bucket.index() + "/" + bucket.bucketId() + ", " + bucket.journalKeys().size() + " object(s)]";
    }
}
