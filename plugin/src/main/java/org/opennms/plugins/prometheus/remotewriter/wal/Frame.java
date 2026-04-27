/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32C;

/**
 * On-disk frame format for WAL segments.
 *
 * <pre>
 * ┌──────────────┬─────────────────────┬────────────────┐
 * │ uint32 (BE)  │   protobuf payload  │  uint32 (BE)   │
 * │  length L    │   … L bytes …       │  CRC32C of     │
 * │              │                     │  payload bytes │
 * └──────────────┴─────────────────────┴────────────────┘
 * </pre>
 *
 * <p>Every frame is self-delimiting so torn-tail recovery is trivial: scan
 * forward, compute the CRC of each payload, stop at the first frame whose
 * CRC does not match or whose length reads past end-of-file. The frame
 * *before* the bad one is the recovery point.
 *
 * <p>Big-endian for the length/CRC header so `xxd` of a segment file shows
 * human-readable sizes at the start of each frame — helpful for post-mortem
 * debugging without protoc.
 */
public final class Frame {

    /** Header overhead per frame: 4 bytes length + 4 bytes CRC32C. */
    public static final int HEADER_BYTES = 8;

    private Frame() { /* utility */ }

    /**
     * Encode a single frame into a newly allocated ByteBuffer positioned at
     * 0 and limit at {@code HEADER_BYTES + payload.length}. The returned
     * buffer is ready to {@code channel.write(...)}.
     */
    public static ByteBuffer encode(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + payload.length)
                                   .order(ByteOrder.BIG_ENDIAN);
        buf.putInt(payload.length);
        buf.put(payload);
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        buf.putInt((int) crc.getValue());
        buf.flip();
        return buf;
    }

    /**
     * Attempt to decode one frame starting at the channel's current
     * position. Returns the payload bytes on success, or null if the frame
     * is torn (insufficient bytes for the declared length, or CRC
     * mismatch). Torn reads leave the channel position at the start of the
     * torn frame — the caller is expected to truncate the file there.
     *
     * <p>Rejects absurd length declarations ({@code length > maxPayload})
     * because a byte-rot in the length prefix of an otherwise-intact file
     * would otherwise cause the reader to attempt an enormous allocation
     * before the CRC check could fire.
     *
     * @param channel    segment file channel positioned at a frame boundary
     * @param maxPayload declared-length sanity cap; frames above this are
     *                   treated as torn
     * @return payload bytes, or null if torn
     * @throws IOException on I/O errors other than end-of-file
     */
    public static byte[] decode(FileChannel channel, int maxPayload) throws IOException {
        long frameStart = channel.position();
        ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        int read = fillFully(channel, header);
        if (read < 4) {
            // End of file before a length prefix — no torn frame; we're at
            // the natural end. Rewind so the caller sees a clean boundary.
            channel.position(frameStart);
            return null;
        }
        header.flip();
        int length = header.getInt();
        if (length < 0 || length > maxPayload) {
            channel.position(frameStart);
            return null;
        }
        ByteBuffer payload = ByteBuffer.allocate(length);
        if (length > 0 && fillFully(channel, payload) < length) {
            channel.position(frameStart);
            return null;
        }
        ByteBuffer crcBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        if (fillFully(channel, crcBuf) < 4) {
            channel.position(frameStart);
            return null;
        }
        crcBuf.flip();
        int expected = crcBuf.getInt();
        CRC32C actual = new CRC32C();
        byte[] payloadBytes = payload.array();
        actual.update(payloadBytes, 0, length);
        if ((int) actual.getValue() != expected) {
            channel.position(frameStart);
            return null;
        }
        return payloadBytes;
    }

    /**
     * Read bytes until the buffer is full or EOF is reached. Returns the
     * number of bytes actually read.
     */
    private static int fillFully(FileChannel channel, ByteBuffer buf) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int n = channel.read(buf);
            if (n < 0) break;
            total += n;
        }
        return total;
    }
}
