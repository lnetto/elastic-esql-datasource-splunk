/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Finds the indexed-field names a dataset carries (what the Python decoder emits as {@code fields}),
 * by decoding events from a few buckets of each index (a volume can hold many indexes with
 * unrelated fields). ES|QL has no map type, so each name becomes its own keyword column, {@code splunk.fields.<name>}; a
 * name that only appears outside the sample can still be added with the {@code fields} setting.
 */
final class FieldDiscovery {

    static final int MAX_INDEXES = 32;
    static final int BUCKETS_PER_INDEX = 8;
    static final int MAX_EVENTS_PER_BUCKET = 20_000;
    /** Stop reading a bucket once this many events in a row added no new field name. */
    static final int STOP_AFTER_NOTHING_NEW = 5_000;

    private FieldDiscovery() {}

    /**
     * Sorted, so the schema doesn't reshuffle when a newer bucket changes first-seen order.
     * Samples the {@link #BUCKETS_PER_INDEX} <b>largest</b> buckets of each index: size tracks
     * sourcetype variety, while the newest buckets are often tiny stragglers (BOTSv3's newest three
     * hold 1, 29 and 2 events of one sourcetype each). Within a bucket, forwarders send file by file,
     * so a CSV after a JSON file only shows up thousands of events in; hence the per-bucket depth.
     */
    static Set<String> sample(BucketStore store, List<BucketDiscovery.BucketRef> buckets) throws IOException {
        Set<String> names = new TreeSet<>();
        Map<String, List<BucketDiscovery.BucketRef>> byIndex = new LinkedHashMap<>();
        for (BucketDiscovery.BucketRef b : buckets) {
            if (b.journalKeys().isEmpty() == false && (byIndex.containsKey(b.index()) || byIndex.size() < MAX_INDEXES)) {
                byIndex.computeIfAbsent(b.index(), k -> new ArrayList<>()).add(b);
            }
        }
        for (List<BucketDiscovery.BucketRef> perIndex : byIndex.values()) {
            perIndex.sort(Comparator.comparingLong(BucketDiscovery.BucketRef::storedBytes).reversed());
            for (BucketDiscovery.BucketRef b : perIndex.subList(0, Math.min(BUCKETS_PER_INDEX, perIndex.size()))) {
                sampleBucket(store, b, names);
            }
        }
        return names;
    }

    private static void sampleBucket(BucketStore store, BucketDiscovery.BucketRef b, Set<String> names) throws IOException {
        try (InputStream in = JournalStreams.open(store::open, b.journalKeys()); JournalDecoder d = new JournalDecoder(in, true)) {
            JournalDecoder.Event e = new JournalDecoder.Event();
            int lastNew = 0;
            for (int n = 0; n < MAX_EVENTS_PER_BUCKET && n - lastNew < STOP_AFTER_NOTHING_NEW && d.next(e); n++) {
                for (int i = 0; i < e.fields.size(); i += 2) {
                    String name = new String(e.fields.get(i), StandardCharsets.UTF_8);
                    if (name.isEmpty() == false && names.add(name)) {
                        lastNew = n;
                    }
                }
            }
        }
    }
}
