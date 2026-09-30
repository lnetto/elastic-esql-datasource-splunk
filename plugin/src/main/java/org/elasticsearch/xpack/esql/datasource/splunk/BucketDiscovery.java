/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds buckets in a {@link BucketStore} listing. Handles the layouts that matter:
 *
 * <pre>
 *   frozen / classic:  [index/]{db,colddb,thaweddb}/db_&lt;newest&gt;_&lt;oldest&gt;_&lt;id&gt;[_&lt;guid&gt;]/rawdata/journal.{gz,zst,lz4}
 *   frozen archive:    &lt;coldToFrozenDir&gt;/{db,rb}_&lt;newest&gt;_&lt;oldest&gt;_&lt;id&gt;_&lt;guid&gt;/rawdata/journal.gz
 *   SmartStore:        &lt;index&gt;/db/xx/yy/&lt;id&gt;~&lt;guid&gt;/guidSplunk-&lt;uploader&gt;/rawdata/{journal.zst | 0, &lt;off&gt;, ...}
 *                      + &lt;id&gt;~&lt;guid&gt;/receipt.json
 * </pre>
 *
 * Deduplication: a SmartStore bucket may carry several {@code guidSplunk-*} copies (receipt.json
 * names the canonical one); a clustered frozen archive holds one copy per replica, as
 * {@code db_} and {@code rb_} dirs with the same {@code <id>_<guid>} — both collapse to one bucket.
 */
final class BucketDiscovery {

    static final long UNKNOWN = -1;

    private static final Pattern CLASSIC = Pattern.compile("^(db|rb)_(\\d+)_(\\d+)_(\\d+)(?:_(.+))?$");
    private static final Pattern SMARTSTORE = Pattern.compile("^(\\d+)~(.+)$");
    private static final Set<String> INDEX_PARENTS = Set.of("db", "colddb", "thaweddb");

    private BucketDiscovery() {}

    /** One logical bucket, ready to decode. */
    record BucketRef(String index, String bucketId, String dir, List<String> journalKeys, long newest, long oldest, long storedBytes) {
        boolean overlaps(long earliest, long latest) {
            if (newest != UNKNOWN && earliest != UNKNOWN && newest < earliest) {
                return false;
            }
            return oldest == UNKNOWN || latest == UNKNOWN || oldest <= latest;
        }
    }

    /**
     * @param indexOverride index name to report for every bucket, or null to derive it from the path
     * @param earliest      prune buckets whose newest event is older than this (epoch s), or {@link #UNKNOWN}
     * @param latest        prune buckets whose oldest event is newer than this (epoch s), or {@link #UNKNOWN}
     */
    static List<BucketRef> discover(BucketStore store, String rootName, String indexOverride, long earliest, long latest)
        throws IOException {
        return discover(store, rootName, indexOverride, null, earliest, latest);
    }

    /** Discovery as configured for a dataset. */
    static List<BucketRef> discover(BucketStore store, SplunkConfig config) throws IOException {
        return discover(store, config.rootName(), config.index(), config.indexPattern(), config.earliest(), config.latest());
    }

    /**
     * @param indexPattern regex applied to the bucket's path (relative to the root); group 1 is the
     *                     index. Buckets it doesn't match fall back to the path rules. May be null.
     */
    static List<BucketRef> discover(
        BucketStore store,
        String rootName,
        String indexOverride,
        Pattern indexPattern,
        long earliest,
        long latest
    ) throws IOException {
        // bucketDir -> contentDir (bucketDir itself, or a guidSplunk-* copy) -> journal entries
        Map<String, Map<String, List<BucketStore.Entry>>> candidates = new TreeMap<>();
        Map<String, String> receipts = new HashMap<>();
        store.list(e -> {
            String key = e.key();
            int slash = key.lastIndexOf('/');
            String name = key.substring(slash + 1);
            String dir = slash < 0 ? "" : key.substring(0, slash);
            if (name.equals("receipt.json")) {
                receipts.put(dir, key);
                return;
            }
            // dir is .../rawdata for real buckets; a loose journal.* makes its own dir the bucket
            String contentDir = lastSegment(dir).equals("rawdata") ? parent(dir) : dir;
            String bucketDir = lastSegment(contentDir).startsWith("guidSplunk") ? parent(contentDir) : contentDir;
            candidates.computeIfAbsent(bucketDir, k -> new TreeMap<>()).computeIfAbsent(contentDir, k -> new ArrayList<>()).add(e);
        });

        Map<String, BucketRef> byIdentity = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, List<BucketStore.Entry>>> b : candidates.entrySet()) {
            String bucketDir = b.getKey();
            String bucketName = bucketDir.isEmpty() ? rootName : lastSegment(bucketDir);
            if (bucketName.startsWith("DISABLED")) {
                continue;                   // Splunk never searches DISABLED-db_* buckets (stale/duplicate copies)
            }
            Map<String, List<BucketStore.Entry>> copies = b.getValue();

            String receiptKey = receipts.get(bucketDir);
            Map<String, Object> receipt = null;
            String contentDir;
            if (copies.size() == 1) {
                contentDir = copies.keySet().iterator().next();
            } else {
                receipt = receiptKey == null ? null : readReceipt(store, receiptKey);
                contentDir = canonicalCopy(copies, receipt);
            }
            List<BucketStore.Entry> files = copies.get(contentDir);
            List<String> journalKeys = journalKeys(files);
            long stored = 0;
            for (BucketStore.Entry f : files) {
                if (journalKeys.contains(f.key()) && f.size() > 0) {
                    stored += f.size();
                }
            }

            long newest = UNKNOWN;
            long oldest = UNKNOWN;
            String identity;
            Matcher classic = CLASSIC.matcher(bucketName);
            Matcher smart = SMARTSTORE.matcher(bucketName);
            if (classic.matches()) {
                newest = Long.parseLong(classic.group(2));
                oldest = Long.parseLong(classic.group(3));
                identity = classic.group(5) == null ? bucketDir : classic.group(4) + "_" + classic.group(5);
            } else if (smart.matches()) {
                identity = smart.group(1) + "_" + smart.group(2);
            } else {
                identity = bucketDir;
            }
            if (newest == UNKNOWN && receiptKey != null && (earliest != UNKNOWN || latest != UNKNOWN)) {
                // only pay for the receipt GET when it can prune
                if (receipt == null) {
                    receipt = readReceipt(store, receiptKey);
                }
                long[] range = receiptRange(receipt);
                if (range != null) {
                    newest = range[0];
                    oldest = range[1];
                }
            }

            String index = indexOverride != null ? indexOverride : indexFor(bucketDir, rootName, indexPattern);
            BucketRef ref = new BucketRef(index, bucketName, bucketDir, journalKeys, newest, oldest, stored);
            if (ref.overlaps(earliest, latest) == false) {
                continue;
            }
            String dedupeKey = index + "\u0000" + identity;
            BucketRef existing = byIdentity.get(dedupeKey);
            // prefer the origin copy (db_) over a replica (rb_)
            if (existing == null || (existing.bucketId().startsWith("rb_") && bucketName.startsWith("db_"))) {
                byIdentity.put(dedupeKey, ref);
            }
        }
        List<BucketRef> out = new ArrayList<>(byIdentity.values());
        // newest first: LIMIT-style queries see recent data first, and unknown ranges go last
        out.sort(Comparator.comparingLong(BucketRef::newest).reversed().thenComparing(BucketRef::dir));
        return out;
    }

    /** The stored objects making up the journal: one journal* file, or the numbered slices in numeric order. */
    static List<String> journalKeys(List<BucketStore.Entry> files) {
        String best = null;
        int bestRank = Integer.MAX_VALUE;
        List<BucketStore.Entry> slices = new ArrayList<>();
        for (BucketStore.Entry f : files) {
            String name = lastSegment(f.key());
            if (name.startsWith("journal")) {
                int rank = switch (name) {
                    case "journal.zst" -> 0;
                    case "journal.gz" -> 1;
                    case "journal.lz4" -> 2;
                    case "journal" -> 3;
                    default -> 4;
                };
                if (rank < bestRank) {
                    bestRank = rank;
                    best = f.key();
                }
            } else {
                slices.add(f);
            }
        }
        if (best != null) {
            return List.of(best);
        }
        slices.sort(Comparator.comparingLong(f -> Long.parseLong(lastSegment(f.key()))));
        return slices.stream().map(BucketStore.Entry::key).toList();
    }

    private static String canonicalCopy(Map<String, List<BucketStore.Entry>> copies, Map<String, Object> receipt) {
        if (receipt != null && receipt.get("objects") instanceof List<?> objects) {
            for (Object o : objects) {
                if (o instanceof Map<?, ?> m && m.get("name") instanceof String name) {
                    for (String copy : copies.keySet()) {
                        String guidDir = lastSegment(copy);
                        if (guidDir.startsWith("guidSplunk") && name.contains("/" + guidDir + "/")) {
                            return copy;
                        }
                    }
                }
            }
        }
        // no receipt verdict: prefer a copy with a whole journal over one with slices
        for (Map.Entry<String, List<BucketStore.Entry>> c : copies.entrySet()) {
            for (BucketStore.Entry f : c.getValue()) {
                if (lastSegment(f.key()).startsWith("journal")) {
                    return c.getKey();
                }
            }
        }
        return copies.keySet().iterator().next();
    }

    static Map<String, Object> readReceipt(BucketStore store, String key) {
        try (InputStream in = store.open(key)) {
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), in, false);
        } catch (Exception e) {
            return null;   // a missing or odd receipt only costs us pruning/dedupe hints
        }
    }

    /**
     * (newest, oldest) from receipt.json's manifest. Real SmartStore receipts (Splunk 10.4) carry the
     * classic bucket name, {@code "path": "db_<newest>_<oldest>_<id>_<guid>"}; otherwise scan for
     * *earliest*time / *latest*time fields.
     */
    static long[] receiptRange(Map<String, Object> receipt) {
        if (receipt == null || receipt.get("manifest") instanceof Map<?, ?> == false) {
            return null;
        }
        Map<?, ?> manifest = (Map<?, ?>) receipt.get("manifest");
        if (manifest.get("path") instanceof String path) {
            Matcher m = CLASSIC.matcher(lastSegment(path));
            if (m.matches()) {
                return new long[] { Long.parseLong(m.group(2)), Long.parseLong(m.group(3)) };
            }
        }
        long earliest = UNKNOWN;
        long latest = UNKNOWN;
        for (Map.Entry<?, ?> e : manifest.entrySet()) {
            String k = String.valueOf(e.getKey()).toLowerCase(Locale.ROOT);
            if (k.contains("time") == false || k.contains("index")) {
                continue;
            }
            try {
                long v = (long) Double.parseDouble(String.valueOf(e.getValue()));
                if (k.contains("earliest")) {
                    earliest = v;
                } else if (k.contains("latest")) {
                    latest = v;
                }
            } catch (NumberFormatException ignored) {
                // not a time value
            }
        }
        return earliest == UNKNOWN || latest == UNKNOWN ? null : new long[] { latest, earliest };
    }

    static String indexFor(String bucketDir, String rootName, Pattern indexPattern) {
        if (indexPattern != null) {
            Matcher m = indexPattern.matcher(bucketDir);
            if (m.find() && m.groupCount() >= 1 && m.group(1) != null && m.group(1).isEmpty() == false) {
                return m.group(1);
            }
        }
        return indexFromPath(bucketDir, rootName);
    }

    /** Component before db/colddb/thaweddb; else the bucket's parent directory; else the root's own name. */
    static String indexFromPath(String bucketDir, String rootName) {
        if (bucketDir.isEmpty()) {
            return "unknown";                                   // the root is a single bucket
        }
        String[] parts = bucketDir.split("/");
        for (int i = parts.length - 1; i >= 0; i--) {
            if (INDEX_PARENTS.contains(parts[i])) {
                return i > 0 ? parts[i - 1] : rootName;
            }
        }
        return parts.length >= 2 ? parts[parts.length - 2] : rootName;
    }

    static String parent(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? "" : path.substring(0, slash);
    }

    static String lastSegment(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }
}
