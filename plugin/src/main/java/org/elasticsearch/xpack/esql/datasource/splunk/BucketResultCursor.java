/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Decodes buckets one after another into ES|QL {@link Page}s. Only the requested columns are
 * materialised; indexed fields are only decoded when one is projected. Stops at the row limit.
 */
final class BucketResultCursor implements ResultCursor {

    static final int DEFAULT_BATCH = 1000;
    /** Flush a page early once this much _raw has accumulated, so huge events don't blow the breaker. */
    static final long MAX_PAGE_RAW_BYTES = 8L * 1024 * 1024;

    private enum Kind {
        TIME,
        FIELD_TIME,
        INDEXTIME,
        INDEX,
        HOST,
        SOURCE,
        LOG_PATH,
        SOURCETYPE,
        RAW,
        BUCKET,
        FIELD
    }

    private final BucketStore store;
    private final Iterator<BucketDiscovery.BucketRef> buckets;
    private final BlockFactory blockFactory;
    private final int batchSize;
    private final long rowLimit;
    private final long earliestMs;
    private final long latestMs;
    private final Kind[] kinds;
    private final byte[][] fieldKeys;
    private final boolean decodeFields;

    private final JournalDecoder.Event event = new JournalDecoder.Event();
    private JournalDecoder decoder;
    private BucketDiscovery.BucketRef current;
    private BytesRef currentIndex;
    private BytesRef currentBucket;
    private long emitted;
    private Page pending;
    private boolean exhausted;
    private volatile boolean cancelled;

    BucketResultCursor(
        BucketStore store,
        List<BucketDiscovery.BucketRef> buckets,
        List<Attribute> attributes,
        BlockFactory blockFactory,
        int batchSize,
        long rowLimit,
        long earliestSeconds,
        long latestSeconds,
        String timestampField
    ) {
        this.store = store;
        this.buckets = buckets.iterator();
        this.blockFactory = blockFactory;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH;
        this.rowLimit = rowLimit < 0 ? Long.MAX_VALUE : rowLimit;
        this.earliestMs = earliestSeconds == BucketDiscovery.UNKNOWN ? Long.MIN_VALUE : earliestSeconds * 1000L;
        this.latestMs = latestSeconds == BucketDiscovery.UNKNOWN ? Long.MAX_VALUE : latestSeconds * 1000L + 999L;
        this.kinds = new Kind[attributes.size()];
        this.fieldKeys = new byte[attributes.size()][];
        boolean anyField = false;
        for (int i = 0; i < attributes.size(); i++) {
            String name = attributes.get(i).name();
            kinds[i] = switch (name) {
                case SplunkSchema.TIMESTAMP -> timestampKind(timestampField);
                case SplunkSchema.MESSAGE -> Kind.RAW;
                case SplunkSchema.HOST_NAME, SplunkSchema.HOST -> Kind.HOST;
                case SplunkSchema.LOG_FILE_PATH -> Kind.LOG_PATH;
                case SplunkSchema.INDEX -> Kind.INDEX;
                case SplunkSchema.SOURCE -> Kind.SOURCE;
                case SplunkSchema.SOURCETYPE -> Kind.SOURCETYPE;
                case SplunkSchema.TIME -> Kind.TIME;
                case SplunkSchema.INDEX_TIME -> Kind.INDEXTIME;
                case SplunkSchema.BUCKET -> Kind.BUCKET;
                default -> Kind.FIELD;
            };
            if (kinds[i] == Kind.FIELD) {
                String key = name.startsWith(SplunkSchema.FIELDS_PREFIX) ? name.substring(SplunkSchema.FIELDS_PREFIX.length()) : name;
                fieldKeys[i] = key.getBytes(StandardCharsets.UTF_8);
                anyField = true;
            } else if (kinds[i] == Kind.FIELD_TIME) {
                fieldKeys[i] = timestampField.getBytes(StandardCharsets.UTF_8);
                anyField = true;
            }
        }
        this.decodeFields = anyField;
    }

    @Override
    public boolean hasNext() {
        if (pending == null && exhausted == false) {
            pending = fill();
            if (pending == null) {
                exhausted = true;
                closeDecoder();
            }
        }
        return pending != null;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page p = pending;
        pending = null;
        return p;
    }

    /** Builds the next page, or returns null when there are no more rows. */
    private Page fill() {
        if (emitted >= rowLimit || cancelled) {
            return null;
        }
        Block.Builder[] builders = new Block.Builder[kinds.length];
        boolean success = false;
        try {
            for (int i = 0; i < kinds.length; i++) {
                builders[i] = switch (kinds[i]) {
                    case TIME, INDEXTIME, FIELD_TIME -> blockFactory.newLongBlockBuilder(batchSize);
                    default -> blockFactory.newBytesRefBlockBuilder(batchSize);
                };
            }
            int rows = 0;
            long rawBytes = 0;
            BytesRef scratch = new BytesRef();
            while (rows < batchSize && emitted < rowLimit && rawBytes < MAX_PAGE_RAW_BYTES && nextEvent()) {
                JournalDecoder.Event e = event;
                for (int i = 0; i < kinds.length; i++) {
                    switch (kinds[i]) {
                        case TIME -> ((LongBlock.Builder) builders[i]).appendLong(e.timeMillis);
                        case INDEXTIME -> ((LongBlock.Builder) builders[i]).appendLong(e.indexTimeSeconds * 1000L);
                        case FIELD_TIME -> appendFieldTime((LongBlock.Builder) builders[i], fieldKeys[i], e.fields);
                        case INDEX -> ((BytesRefBlock.Builder) builders[i]).appendBytesRef(currentIndex);
                        case HOST -> ((BytesRefBlock.Builder) builders[i]).appendBytesRef(ref(scratch, e.host, e.host.length));
                        case SOURCE -> ((BytesRefBlock.Builder) builders[i]).appendBytesRef(ref(scratch, e.source, e.source.length));
                        case LOG_PATH -> {
                            if (SplunkSchema.isFilePath(e.source)) {
                                ((BytesRefBlock.Builder) builders[i]).appendBytesRef(ref(scratch, e.source, e.source.length));
                            } else {
                                builders[i].appendNull();
                            }
                        }
                        case SOURCETYPE -> ((BytesRefBlock.Builder) builders[i]).appendBytesRef(
                            ref(scratch, e.sourcetype, e.sourcetype.length)
                        );
                        case RAW -> ((BytesRefBlock.Builder) builders[i]).appendBytesRef(ref(scratch, e.raw, e.rawLength));
                        case BUCKET -> ((BytesRefBlock.Builder) builders[i]).appendBytesRef(currentBucket);
                        case FIELD -> appendField((BytesRefBlock.Builder) builders[i], fieldKeys[i], e.fields, scratch);
                    }
                }
                rows++;
                emitted++;
                rawBytes += e.rawLength;
            }
            if (rows == 0) {
                return null;
            }
            Block[] blocks = new Block[builders.length];
            try {
                for (int i = 0; i < builders.length; i++) {
                    blocks[i] = builders[i].build();
                }
            } catch (RuntimeException ex) {
                Releasables.closeExpectNoException(blocks);
                throw ex;
            }
            success = true;
            return new Page(rows, blocks);
        } finally {
            Releasables.closeExpectNoException(builders);
            if (success == false) {
                closeDecoder();
            }
        }
    }

    private static BytesRef ref(BytesRef scratch, byte[] bytes, int length) {
        scratch.bytes = bytes;
        scratch.offset = 0;
        scratch.length = length;
        return scratch;
    }

    /** {@code @timestamp} from {@code timestamp_field}: _time (default), _indextime, or an indexed field. */
    private static Kind timestampKind(String timestampField) {
        if (timestampField == null || timestampField.equals(SplunkConfig.TIME)) {
            return Kind.TIME;
        }
        return timestampField.equals(SplunkConfig.INDEX_TIME) ? Kind.INDEXTIME : Kind.FIELD_TIME;
    }

    /** First value of the field that parses as a timestamp; null when absent or unparseable. */
    private static void appendFieldTime(LongBlock.Builder b, byte[] key, List<byte[]> fields) {
        for (int i = 0; i < fields.size(); i += 2) {
            if (Arrays.equals(fields.get(i), key)) {
                Long ms = TimestampParser.parseMillis(fields.get(i + 1));
                if (ms != null) {
                    b.appendLong(ms);
                    return;
                }
            }
        }
        b.appendNull();
    }

    private static void appendField(BytesRefBlock.Builder b, byte[] key, List<byte[]> fields, BytesRef scratch) {
        int matches = 0;
        for (int i = 0; i < fields.size(); i += 2) {
            if (Arrays.equals(fields.get(i), key)) {
                matches++;
            }
        }
        if (matches == 0) {
            b.appendNull();
            return;
        }
        if (matches > 1) {
            b.beginPositionEntry();
        }
        for (int i = 0; i < fields.size(); i += 2) {
            if (Arrays.equals(fields.get(i), key)) {
                byte[] v = fields.get(i + 1);
                b.appendBytesRef(ref(scratch, v, v.length));
            }
        }
        if (matches > 1) {
            b.endPositionEntry();
        }
    }

    /** Advances to the next in-window event across buckets. */
    private boolean nextEvent() {
        while (cancelled == false) {
            if (decoder == null && openNextBucket() == false) {
                return false;
            }
            try {
                if (decoder.next(event)) {
                    if (event.timeMillis >= earliestMs && event.timeMillis <= latestMs) {
                        return true;
                    }
                    continue;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "failed decoding Splunk bucket [" + current.index() + "/" + current.bucketId() + "] from " + store.describe()
                        + " at journal offset " + decoder.position() + ": " + e.getMessage(),
                    e
                );
            }
            closeDecoder();
        }
        return false;
    }

    private boolean openNextBucket() {
        do {
            if (buckets.hasNext() == false) {
                return false;
            }
            current = buckets.next();
        } while (current.journalKeys().isEmpty());          // the "everything pruned" placeholder split
        currentIndex = new BytesRef(current.index());
        currentBucket = new BytesRef(current.bucketId());
        try {
            decoder = new JournalDecoder(JournalStreams.open(store::open, current.journalKeys()), decodeFields);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "failed opening Splunk bucket [" + current.index() + "/" + current.bucketId() + "] from " + store.describe(),
                e
            );
        }
        return true;
    }

    private void closeDecoder() {
        if (decoder != null) {
            try {
                decoder.close();
            } catch (IOException e) {
                // best effort
            }
            decoder = null;
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void close() {
        closeDecoder();
        if (pending != null) {
            pending.releaseBlocks();
            pending = null;
        }
    }
}
