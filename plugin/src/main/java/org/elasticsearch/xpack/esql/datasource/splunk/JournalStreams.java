/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdInputStream;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * Turns the stored bytes of a bucket's journal into the decompressed journal stream.
 *
 * <p>Splunk writes {@code rawdata/journal.gz}, {@code journal.zst} or {@code journal.lz4}
 * depending on {@code journalCompression}. SmartStore uploads may instead store the journal
 * as numbered slices ({@code rawdata/0}, {@code rawdata/<offset>}...), each a complete
 * compressed frame; frames concatenate, so the slices are streamed back-to-back in numeric
 * order. Compression is always detected from magic bytes, never trusted from the file name.
 *
 * <p>zstd and LZ4 come from aircompressor (pure Java), provided at runtime by the
 * {@code esql-datasource-compression-libs} module this plugin extends.
 */
final class JournalStreams {

    private JournalStreams() {}

    /** Opens one stored object. Implemented by the filesystem and S3 bucket stores. */
    @FunctionalInterface
    interface Opener {
        InputStream open(String key) throws IOException;
    }

    enum Codec {
        GZIP,
        ZSTD,
        LZ4,
        NONE
    }

    static final int LZ4_FRAME_MAGIC = 0x184D2204;
    static final int LZ4_LEGACY_MAGIC = 0x184C2102;

    /**
     * Decompressed journal for a bucket whose journal is stored as {@code keys}
     * (a single journal object, or slice objects already sorted in numeric order).
     */
    static InputStream open(Opener opener, List<String> keys) throws IOException {
        InputStream stored = keys.size() == 1 ? opener.open(keys.get(0)) : new LazyConcat(opener, keys.iterator());
        return decompress(new BufferedInputStream(stored, 256 * 1024));
    }

    static InputStream decompress(BufferedInputStream in) throws IOException {
        return switch (sniff(in)) {
            case GZIP -> new GzipMembersInputStream(in);         // handles concatenated members
            case ZSTD -> new ZstdInputStream(in);                // handles concatenated frames
            case LZ4 -> new Lz4FrameInputStream(in);
            case NONE -> in;
        };
    }

    static Codec sniff(BufferedInputStream in) throws IOException {
        in.mark(4);
        byte[] m = in.readNBytes(4);
        in.reset();
        if (m.length >= 2 && (m[0] & 0xFF) == 0x1F && (m[1] & 0xFF) == 0x8B) {
            return Codec.GZIP;
        }
        if (m.length == 4) {
            int le = (m[0] & 0xFF) | (m[1] & 0xFF) << 8 | (m[2] & 0xFF) << 16 | (m[3] & 0xFF) << 24;
            if (le == 0xFD2FB528) {
                return Codec.ZSTD;
            }
            if (le == LZ4_FRAME_MAGIC || le == LZ4_LEGACY_MAGIC) {
                return Codec.LZ4;
            }
        }
        return Codec.NONE;
    }

    /** Opens slice objects one at a time, so only one remote stream is ever live. */
    private static final class LazyConcat extends InputStream {
        private final Opener opener;
        private final Iterator<String> keys;
        private InputStream current;

        LazyConcat(Opener opener, Iterator<String> keys) {
            this.opener = opener;
            this.keys = keys;
        }

        private boolean advance() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
            if (keys.hasNext() == false) {
                return false;
            }
            current = opener.open(keys.next());
            return true;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            while (true) {
                if (current == null && advance() == false) {
                    return -1;
                }
                int n = current.read(b, off, len);
                if (n >= 0) {
                    return n;
                }
                if (advance() == false) {
                    return -1;
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (current != null) {
                current.close();
            }
        }
    }

    /**
     * Multi-member gzip reader. Splunk writes {@code journal.gz} as many small members (~14 KB
     * each), so a 3 GB journal has hundreds of thousands of member boundaries.
     *
     * <p>{@link java.util.zip.GZIPInputStream} only continues past a member when
     * {@code in.available() > 0} (or more than 26 bytes are left in its inflater). A network
     * stream routinely reports 0 at the boundary, and GZIPInputStream then returns a clean EOF:
     * the journal is silently cut short at a random member. This reader decides from the bytes
     * alone: after a member it reads ahead, and only a true end of input ends the stream.
     * A truncated member throws instead of ending quietly.
     */
    static final class GzipMembersInputStream extends InputStream {
        private static final int FHCRC = 2, FEXTRA = 4, FNAME = 8, FCOMMENT = 16;

        private final InputStream in;
        private final Inflater inflater = new Inflater(true);
        private final CRC32 crc = new CRC32();
        private final byte[] input = new byte[64 * 1024];
        private int inputPos;
        private int inputLen;
        private boolean eof;

        GzipMembersInputStream(InputStream in) throws IOException {
            this.in = in;
            if (readHeader(true) == false) {
                throw new EOFException("empty gzip stream");
            }
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            while (eof == false) {
                int n;
                try {
                    n = inflater.inflate(b, off, len);
                } catch (DataFormatException e) {
                    throw new ZipException("invalid gzip data: " + e.getMessage());
                }
                if (n > 0) {
                    crc.update(b, off, n);
                    return n;
                }
                if (inflater.finished()) {
                    inputPos = inputLen - inflater.getRemaining();
                    readTrailer();
                    if (readHeader(false) == false) {
                        eof = true;
                    }
                } else if (inflater.needsInput()) {
                    if (fill() == false) {
                        throw new EOFException("truncated gzip member");
                    }
                    inflater.setInput(input, inputPos, inputLen - inputPos);
                } else if (inflater.needsDictionary()) {
                    throw new ZipException("gzip member needs a preset dictionary");
                }
            }
            return -1;
        }

        /** Next member's header; false on a clean end of input (or trailing non-gzip bytes, which gzip(1) also ignores). */
        private boolean readHeader(boolean first) throws IOException {
            int b0 = nextByte();
            if (b0 < 0) {
                return false;
            }
            int b1 = nextByte();
            if (b0 != 0x1F || b1 != 0x8B) {
                if (first) {
                    throw new ZipException("not in gzip format");
                }
                return false;
            }
            CRC32 headerCrc = new CRC32();
            headerCrc.update(b0);
            headerCrc.update(b1);
            int cm = requireByte(headerCrc);
            if (cm != 8) {
                throw new ZipException("unsupported gzip compression method " + cm);
            }
            int flg = requireByte(headerCrc);
            for (int i = 0; i < 6; i++) {                   // MTIME, XFL, OS
                requireByte(headerCrc);
            }
            if ((flg & FEXTRA) != 0) {
                int xlen = requireByte(headerCrc) | requireByte(headerCrc) << 8;
                for (int i = 0; i < xlen; i++) {
                    requireByte(headerCrc);
                }
            }
            if ((flg & FNAME) != 0) {
                while (requireByte(headerCrc) != 0) {
                }
            }
            if ((flg & FCOMMENT) != 0) {
                while (requireByte(headerCrc) != 0) {
                }
            }
            if ((flg & FHCRC) != 0) {
                int expected = requireByte(null) | requireByte(null) << 8;
                if (expected != ((int) headerCrc.getValue() & 0xFFFF)) {
                    throw new ZipException("corrupt gzip header");
                }
            }
            inflater.reset();
            crc.reset();
            inflater.setInput(input, inputPos, inputLen - inputPos);
            return true;
        }

        private void readTrailer() throws IOException {
            long expectedCrc = readUInt();
            long expectedSize = readUInt();
            if (expectedCrc != crc.getValue()) {
                throw new ZipException("corrupt gzip member (CRC mismatch)");
            }
            if (expectedSize != (inflater.getBytesWritten() & 0xFFFFFFFFL)) {
                throw new ZipException("corrupt gzip member (size mismatch)");
            }
        }

        private long readUInt() throws IOException {
            long v = 0;
            for (int i = 0; i < 4; i++) {
                v |= (long) requireByte(null) << (8 * i);
            }
            return v;
        }

        private int requireByte(CRC32 headerCrc) throws IOException {
            int b = nextByte();
            if (b < 0) {
                throw new EOFException("truncated gzip member");
            }
            if (headerCrc != null) {
                headerCrc.update(b);
            }
            return b;
        }

        private int nextByte() throws IOException {
            if (inputPos == inputLen && fill() == false) {
                return -1;
            }
            return input[inputPos++] & 0xFF;
        }

        /** Refills the input buffer once it is fully consumed; false only at end of input. */
        private boolean fill() throws IOException {
            int n = in.read(input, 0, input.length);
            while (n == 0) {
                n = in.read(input, 0, input.length);
            }
            if (n < 0) {
                inputPos = inputLen = 0;
                return false;
            }
            inputPos = 0;
            inputLen = n;
            return true;
        }

        @Override
        public void close() throws IOException {
            inflater.end();
            in.close();
        }
    }

    /**
     * LZ4 frame format (and the legacy frame format) reader over aircompressor's block
     * decompressor. Handles concatenated frames and skippable frames; checksums are skipped,
     * not verified — the journal decoder's structural checks catch corruption.
     */
    static final class Lz4FrameInputStream extends InputStream {
        private static final int LEGACY_BLOCK = 8 * 1024 * 1024;

        private final InputStream in;
        private final Lz4Decompressor decompressor = new Lz4Decompressor();
        private byte[] compressed = new byte[0];
        private byte[] block = new byte[0];
        private int blockPos;
        private int blockLen;

        private boolean inFrame;
        private boolean legacy;
        private boolean blockChecksum;
        private boolean contentChecksum;
        private int maxBlock;

        Lz4FrameInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            if (blockPos == blockLen && nextBlock() == false) {
                return -1;
            }
            return block[blockPos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (blockPos == blockLen && nextBlock() == false) {
                return -1;
            }
            int n = Math.min(len, blockLen - blockPos);
            System.arraycopy(block, blockPos, b, off, n);
            blockPos += n;
            return n;
        }

        private boolean nextBlock() throws IOException {
            while (true) {
                if (inFrame == false && startFrame() == false) {
                    return false;
                }
                long word = readIntLE(legacy);
                if (word < 0) {
                    if (legacy) {                         // legacy frames end at EOF
                        return false;
                    }
                    throw new EOFException("truncated LZ4 frame");
                }
                int size = (int) word;
                if (legacy && size == LZ4_LEGACY_MAGIC) {  // next legacy frame
                    continue;
                }
                if (legacy == false && size == 0) {       // EndMark
                    if (contentChecksum) {
                        in.skipNBytes(4);
                    }
                    inFrame = false;
                    continue;
                }
                boolean uncompressed = legacy == false && (size & 0x80000000) != 0;
                size &= 0x7FFFFFFF;
                if (size > maxBlock + maxBlock / 255 + 16) {   // LZ4 worst-case expansion
                    throw new IOException("LZ4 block size " + size + " exceeds frame maximum " + maxBlock);
                }
                if (block.length < maxBlock) {
                    block = new byte[maxBlock];
                }
                if (uncompressed) {
                    readFully(block, size);
                    blockLen = size;
                } else {
                    if (compressed.length < size) {
                        compressed = new byte[size];
                    }
                    readFully(compressed, size);
                    blockLen = decompressor.decompress(compressed, 0, size, block, 0, block.length);
                }
                if (blockChecksum) {
                    in.skipNBytes(4);
                }
                blockPos = 0;
                if (blockLen > 0) {
                    return true;
                }
            }
        }

        /** Reads a frame header; false on clean EOF between frames. */
        private boolean startFrame() throws IOException {
            while (true) {
                long magic = readIntLE(true);
                if (magic < 0) {
                    return false;
                }
                if ((magic & 0xFFFFFFF0L) == 0x184D2A50L) {   // skippable frame
                    long len = readIntLE(false);
                    in.skipNBytes(len);
                    continue;
                }
                if (magic == (LZ4_LEGACY_MAGIC & 0xFFFFFFFFL)) {
                    legacy = true;
                    blockChecksum = false;
                    contentChecksum = false;
                    maxBlock = LEGACY_BLOCK;
                    inFrame = true;
                    return true;
                }
                if (magic != (LZ4_FRAME_MAGIC & 0xFFFFFFFFL)) {
                    throw new IOException(String.format(java.util.Locale.ROOT, "not an LZ4 frame (magic 0x%08X)", magic));
                }
                int flg = readByteOrThrow();
                int bd = readByteOrThrow();
                if ((flg >>> 6) != 1) {
                    throw new IOException("unsupported LZ4 frame version " + (flg >>> 6));
                }
                legacy = false;
                blockChecksum = (flg & 0x10) != 0;
                contentChecksum = (flg & 0x04) != 0;
                boolean contentSize = (flg & 0x08) != 0;
                boolean dictId = (flg & 0x01) != 0;
                maxBlock = switch ((bd >>> 4) & 0x7) {
                    case 4 -> 64 * 1024;
                    case 5 -> 256 * 1024;
                    case 6 -> 1024 * 1024;
                    case 7 -> 4 * 1024 * 1024;
                    default -> throw new IOException("invalid LZ4 block max size id " + ((bd >>> 4) & 0x7));
                };
                in.skipNBytes((contentSize ? 8 : 0) + (dictId ? 4 : 0) + 1); // + header checksum
                inFrame = true;
                return true;
            }
        }

        /** Little-endian uint32, or -1 on EOF at the first byte (when allowed). */
        private long readIntLE(boolean eofOk) throws IOException {
            int b0 = in.read();
            if (b0 < 0) {
                if (eofOk) {
                    return -1;
                }
                throw new EOFException("truncated LZ4 stream");
            }
            int b1 = readByteOrThrow();
            int b2 = readByteOrThrow();
            int b3 = readByteOrThrow();
            return (b0 | b1 << 8 | b2 << 16 | (long) b3 << 24) & 0xFFFFFFFFL;
        }

        private int readByteOrThrow() throws IOException {
            int b = in.read();
            if (b < 0) {
                throw new EOFException("truncated LZ4 stream");
            }
            return b;
        }

        private void readFully(byte[] dst, int len) throws IOException {
            int n = in.readNBytes(dst, 0, len);
            if (n != len) {
                throw new EOFException("truncated LZ4 block");
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
