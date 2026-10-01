/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming decoder for the (decompressed) {@code rawdata/journal} data format, so that
 * customer-owned bucket data can be read by Elasticsearch without Splunk software.
 *
 * <p>Port of the Python decoder in splunk-journal-extractor
 * ({@code decoder.py}), with three deliberate differences:
 * <ul>
 *   <li>Streaming: bounded read-ahead buffer, never the whole journal in memory.</li>
 *   <li>Fail fast: an unknown opcode, an over-long varint, or an event whose declared
 *       end lies behind the cursor throws {@link CorruptJournalException} instead of
 *       silently desynchronising and emitting garbage events.</li>
 *   <li>Opcode 0 is explicitly treated as padding (real journals carry a lot of it).</li>
 * </ul>
 *
 * <p>Usage: {@code while (decoder.next(event)) { ... }} — the {@link Event} is reused
 * between calls, so copy anything that must outlive the next call.
 */
final class JournalDecoder implements Closeable {

    static final int PAD = 0;
    static final int NEW_HOST = 3;
    static final int NEW_SOURCE = 4;
    static final int NEW_SOURCETYPE = 5;
    static final int NEW_STRING = 6;
    static final int PRIVATE = 9;
    static final int HEADER = 10;
    /** Slice hash (enableDataIntegrityControl): 32-byte SHA-256, same bytes as rawdata/l1Hashes_*.dat. */
    static final int HASH_SLICE = 11;
    static final int HASH_SLICE_BYTES = 32;

    static final int TYPE_STRING = 0;
    static final int TYPE_LEN_OFFSET = 4;
    static final int TYPE_UNSIGNED = 8;
    static final int TYPE_NEGATIVE = 9;

    /** metadata type nibble -> number of value varints (unknown types carry 0). */
    private static final int[] METADATA_VALUES = { 1, 0, 1, 2, 2, 0, 2, 3, 1, 1, 1, 2, 3, 0, 2, 0 };

    private static final byte[] HOST_PREFIX = "host::".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SOURCE_PREFIX = "source::".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SOURCETYPE_PREFIX = "sourcetype::".getBytes(StandardCharsets.UTF_8);

    /** Guard against a corrupt length field asking us to allocate gigabytes. */
    static final int MAX_EVENT_BYTES = 256 * 1024 * 1024;

    private final InputStream in;
    private final boolean decodeFields;
    private final byte[] buf = new byte[64 * 1024];
    private int bufPos;
    private int bufLen;
    private long position;

    private final List<byte[]> hosts = new ArrayList<>();
    private final List<byte[]> sources = new ArrayList<>();
    private final List<byte[]> sourcetypes = new ArrayList<>();
    private final List<byte[]> strings = new ArrayList<>();

    private byte[] activeHost = Event.EMPTY;
    private byte[] activeSource = Event.EMPTY;
    private byte[] activeSourcetype = Event.EMPTY;
    private long baseEventTime;
    private long baseIndexTime;

    JournalDecoder(InputStream in, boolean decodeFields) {
        this.in = in;
        this.decodeFields = decodeFields;
    }

    /** A decoded event. Byte arrays for host/source/sourcetype are shared table entries — do not mutate. */
    static final class Event {
        static final byte[] EMPTY = new byte[0];

        /** Event time, epoch milliseconds. */
        long timeMillis;
        /** Index time, epoch seconds. */
        long indexTimeSeconds;
        byte[] host = EMPTY;
        byte[] source = EMPTY;
        byte[] sourcetype = EMPTY;
        byte[] raw = EMPTY;
        int rawLength;
        /** Sub-second part of the event time, microseconds. */
        long subsecondMicros;
        /** Indexed-field key/value pairs (flattened k0,v0,k1,v1...); only populated when fields are decoded. */
        final List<byte[]> fields = new ArrayList<>();
        /** Scratch: pending (length, offset) pairs for fields that slice _raw. */
        final List<Long> slices = new ArrayList<>();

        String rawString() {
            return new String(raw, 0, rawLength, StandardCharsets.UTF_8);
        }
    }

    /** Current byte offset into the decompressed journal (for error messages). */
    long position() {
        return position;
    }

    /**
     * Advance to the next event. Returns false at a clean end of journal.
     */
    boolean next(Event event) throws IOException {
        while (true) {
            int opcode = readOpcode();
            if (opcode < 0) {
                return false;
            }
            if (opcode == 1 || opcode == 2 || (opcode >= 32 && opcode <= 43)) {
                decodeEvent(opcode, event);
                return true;
            } else if (opcode >= 17 && opcode <= 31) {
                stateChange(opcode);
            } else if (opcode >= NEW_HOST && opcode <= NEW_STRING) {
                newString(opcode);
            } else if (opcode == HEADER) {
                // bytes 0-1: version, align bits; bytes 2-5: base index time (int32 LE)
                skip(2);
                baseIndexTime = readInt32LE();
            } else if (opcode == PRIVATE) {
                skip(readUVarint());
            } else if (opcode == HASH_SLICE) {
                skip(HASH_SLICE_BYTES);
            } else if (opcode != PAD) {
                throw new CorruptJournalException("unknown journal opcode [" + opcode + "] at offset " + (position - 1));
            }
        }
    }

    private void stateChange(int opcode) throws IOException {
        if ((opcode & 0x8) != 0) {
            activeHost = lookup(hosts, readUVarint());
        }
        if ((opcode & 0x4) != 0) {
            activeSource = lookup(sources, readUVarint());
        }
        if ((opcode & 0x2) != 0) {
            activeSourcetype = lookup(sourcetypes, readUVarint());
        }
        if ((opcode & 0x1) != 0) {
            baseEventTime = readInt32LE();
        }
    }

    private void newString(int opcode) throws IOException {
        int len = checkedLength(readUVarint(), "string");
        byte[] s = readBytes(len);
        switch (opcode) {
            case NEW_HOST -> hosts.add(stripPrefix(s, HOST_PREFIX));
            case NEW_SOURCE -> sources.add(stripPrefix(s, SOURCE_PREFIX));
            case NEW_SOURCETYPE -> sourcetypes.add(stripPrefix(s, SOURCETYPE_PREFIX));
            default -> strings.add(s);
        }
    }

    private void decodeEvent(int opcode, Event e) throws IOException {
        long messageEnd = readUVarint();
        messageEnd += position;
        long extLen = (opcode & 0x4) != 0 ? readUVarint() : 0;
        if ((opcode & 0x1) == 0) {
            skip(20);                    // hash
        }
        skip(8);                         // stream id
        readUVarint();                   // stream offset
        readUVarint();                   // stream sub-offset
        long indexTimeDiff = readUVarint();
        long subSeconds = readUVarint();
        long metaCount = readUVarint();

        e.fields.clear();
        e.slices.clear();
        int pendingSlices = 0;
        for (long m = 0; m < metaCount; m++) {
            long metaKey = readUVarint();
            int type;
            if (opcode <= 2) {
                metaKey <<= 3;
                type = -1;                                  // old-style: always one string ref
            } else {
                if (opcode < 36) {
                    metaKey <<= 2;
                }
                type = (int) (metaKey & 0xF);
            }
            long keyIndex = metaKey >>> 4;
            int numValues = type < 0 ? 1 : METADATA_VALUES[type];
            long v0 = 0;
            long v1 = 0;
            for (int v = 0; v < numValues; v++) {
                long value = readUVarint();
                if (v == 0) {
                    v0 = value;
                } else if (v == 1) {
                    v1 = value;
                }
            }
            if (decodeFields == false || keyIndex <= 0 || keyIndex > strings.size()) {
                continue;
            }
            byte[] key = strings.get((int) keyIndex - 1);
            switch (type) {
                // Confirmed against Splunk 10.4 journals (indexed extractions, WRITE_META, JSON):
                case -1, TYPE_STRING -> {                     // string-table ref
                    if (v0 > 0 && v0 <= strings.size()) {
                        e.fields.add(key);
                        e.fields.add(strings.get((int) v0 - 1));
                    }
                }
                case TYPE_UNSIGNED -> {                       // unsigned literal
                    e.fields.add(key);
                    e.fields.add(Long.toUnsignedString(v0).getBytes(StandardCharsets.US_ASCII));
                }
                case TYPE_NEGATIVE -> {                       // negative integer, stored as its magnitude (date_zone -240 → 240)
                    e.fields.add(key);
                    e.fields.add((v0 == 0 ? "0" : "-" + Long.toUnsignedString(v0)).getBytes(StandardCharsets.US_ASCII));
                }
                case TYPE_LEN_OFFSET -> {                     // (length, offset) into _raw; resolved below
                    e.fields.add(key);
                    e.fields.add(null);
                    e.slices.add(v0);
                    e.slices.add(v1);
                    pendingSlices++;
                }
                // Float/encoded-offset types never appeared in real journals we could produce;
                // their varints are consumed above and the field is skipped, not guessed.
                default -> {
                }
            }
        }
        skip(extLen);
        long msgLen = messageEnd - position;
        if (msgLen < 0) {
            throw new CorruptJournalException("event at offset " + position + " ends before its header (" + msgLen + ")");
        }
        int len = checkedLength(msgLen, "event");
        if (e.raw.length < len) {
            e.raw = new byte[Math.max(len, Math.min(MAX_EVENT_BYTES, e.raw.length * 2))];
        }
        readFully(e.raw, len);
        e.rawLength = len;
        if (pendingSlices > 0) {
            resolveSlices(e);
        }

        e.subsecondMicros = subsecondMicros(subSeconds);
        e.timeMillis = baseEventTime * 1000L + e.subsecondMicros / 1000;
        // The index-time delta is a signed 32-bit value stored as an unsigned varint: events indexed
        // before the header's base time carry 2^32 - n. Adding it raw lands them ~136 years ahead.
        e.indexTimeSeconds = baseIndexTime + (int) indexTimeDiff;
        e.host = activeHost;
        e.source = activeSource;
        e.sourcetype = activeSourcetype;
    }

    /** Fills the null value placeholders of (length, offset) fields with the slice of _raw they point at. */
    private static void resolveSlices(Event e) {
        int s = 0;
        for (int i = 1; i < e.fields.size(); i += 2) {
            if (e.fields.get(i) != null) {
                continue;
            }
            long len = e.slices.get(s++);
            long off = e.slices.get(s++);
            if (off >= 0 && len >= 0 && off + len <= e.rawLength) {
                e.fields.set(i, Arrays.copyOfRange(e.raw, (int) off, (int) (off + len)));
            } else {
                e.fields.set(i, Event.EMPTY);   // out of range: keep the key, don't invent a value
            }
        }
        e.slices.clear();
    }

    /**
     * Sub-second part of _time in microseconds. Verified against Splunk 10.4 for ms, µs and ns
     * TIME_FORMATs: the value is {@code (2*digits + 1) << z}, where {@code digits} is the fraction
     * without leading zeros and {@code z} counts those leading zeros. So the fraction has
     * {@code decimalDigits(digits) + z} places; 0 means no sub-second part. (Splunk keeps at most µs.)
     */
    static long subsecondMicros(long raw) {
        if (raw == 0) {
            return 0;
        }
        int z = Long.numberOfTrailingZeros(raw);
        long digits = (raw >>> z) >>> 1;
        int places = z + (digits == 0 ? 0 : Long.toString(digits).length());
        if (places <= 6) {
            long scaled = digits;
            for (int i = places; i < 6; i++) {
                scaled *= 10;
            }
            return scaled;
        }
        long scaled = digits;
        for (int i = 6; i < places && scaled > 0; i++) {
            scaled /= 10;
        }
        return scaled;
    }

    // ---------------------------------------------------------------------------------
    // Primitive reads over a bounded buffer
    // ---------------------------------------------------------------------------------

    private boolean fill() throws IOException {
        bufPos = 0;
        bufLen = in.read(buf, 0, buf.length);
        if (bufLen <= 0) {
            bufLen = 0;
            return false;
        }
        return true;
    }

    /** Next opcode byte, or -1 on clean EOF. */
    private int readOpcode() throws IOException {
        if (bufPos == bufLen && fill() == false) {
            return -1;
        }
        position++;
        return buf[bufPos++] & 0xFF;
    }

    private int readByte() throws IOException {
        if (bufPos == bufLen && fill() == false) {
            throw new EOFException("journal truncated at offset " + position);
        }
        position++;
        return buf[bufPos++] & 0xFF;
    }

    long readUVarint() throws IOException {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            int b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new CorruptJournalException("varint longer than 10 bytes at offset " + position);
    }

    private int readInt32LE() throws IOException {
        return readByte() | (readByte() << 8) | (readByte() << 16) | (readByte() << 24);
    }

    private void skip(long n) throws IOException {
        while (n > 0) {
            if (bufPos == bufLen && fill() == false) {
                throw new EOFException("journal truncated at offset " + position);
            }
            int step = (int) Math.min(n, bufLen - bufPos);
            bufPos += step;
            position += step;
            n -= step;
        }
    }

    private void readFully(byte[] dst, int len) throws IOException {
        int off = 0;
        while (off < len) {
            if (bufPos == bufLen && fill() == false) {
                throw new EOFException("journal truncated at offset " + position);
            }
            int step = Math.min(len - off, bufLen - bufPos);
            System.arraycopy(buf, bufPos, dst, off, step);
            bufPos += step;
            off += step;
            position += step;
        }
    }

    private byte[] readBytes(int len) throws IOException {
        byte[] out = new byte[len];
        readFully(out, len);
        return out;
    }

    private int checkedLength(long len, String what) throws CorruptJournalException {
        if (len < 0 || len > MAX_EVENT_BYTES) {
            throw new CorruptJournalException(what + " length " + len + " out of range at offset " + position);
        }
        return (int) len;
    }

    private static byte[] lookup(List<byte[]> table, long oneBasedIndex) {
        if (oneBasedIndex <= 0 || oneBasedIndex > table.size()) {
            return Event.EMPTY;
        }
        return table.get((int) oneBasedIndex - 1);
    }

    private static byte[] stripPrefix(byte[] s, byte[] prefix) {
        if (s.length < prefix.length) {
            return s;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (s[i] != prefix[i]) {
                return s;
            }
        }
        byte[] out = new byte[s.length - prefix.length];
        System.arraycopy(s, prefix.length, out, 0, out.length);
        return out;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    /** Structural corruption in the journal (as opposed to plain I/O failure). */
    static final class CorruptJournalException extends IOException {
        CorruptJournalException(String message) {
            super(message);
        }
    }
}
