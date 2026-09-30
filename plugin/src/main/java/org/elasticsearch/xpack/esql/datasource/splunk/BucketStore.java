/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Read-only view of a tree of Splunk buckets. Keys are '/'-separated and relative to the
 * store root, e.g. {@code main/db/2a/48/11~GUID/guidSplunk-GUID/rawdata/journal.zst}.
 */
interface BucketStore extends Closeable {

    record Entry(String key, long size) {}

    /** Stream every object under the root that bucket discovery may care about. */
    void list(Consumer<Entry> sink) throws IOException;

    InputStream open(String key) throws IOException;

    /** Human-readable root, for error messages. */
    String describe();

    /**
     * True for the object names discovery needs; lets stores skip tsidx and friends early:
     * receipt.json, anything under a {@code rawdata/} dir named journal* or all digits (SmartStore
     * slices), and a compressed {@code journal.gz|zst|lz4} sitting in any directory (hand-rolled
     * frozen archives that dropped the rawdata/ level).
     */
    static boolean interesting(String key) {
        int slash = key.lastIndexOf('/');
        String name = key.substring(slash + 1);
        if (name.equals("receipt.json") || LOOSE_JOURNALS.contains(name)) {
            return true;
        }
        if (slash < 0) {
            return false;
        }
        String parent = key.substring(key.lastIndexOf('/', slash - 1) + 1, slash);
        if (parent.equals("rawdata") == false) {
            return false;
        }
        return name.startsWith("journal") || isDigits(name);
    }

    Set<String> LOOSE_JOURNALS = Set.of("journal.gz", "journal.zst", "journal.lz4");

    static boolean isDigits(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}
