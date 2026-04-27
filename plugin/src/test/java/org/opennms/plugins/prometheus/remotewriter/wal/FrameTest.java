/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>, <ronny@no42.org>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FrameTest {

    private static final int MAX_PAYLOAD = 1 << 20; // 1 MiB — plenty for tests

    @Test
    void encode_then_decode_round_trips(@TempDir Path dir) throws IOException {
        byte[] original = "hello, wal".getBytes();
        Path file = dir.resolve("one-frame.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            ch.write(Frame.encode(original));
            ch.position(0);
            byte[] decoded = Frame.decode(ch, MAX_PAYLOAD);
            assertThat(decoded).isEqualTo(original);
        }
    }

    @Test
    void multiple_frames_decode_in_order(@TempDir Path dir) throws IOException {
        byte[][] payloads = {"one".getBytes(), "two".getBytes(), "three".getBytes()};
        Path file = dir.resolve("multi.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            for (byte[] p : payloads) ch.write(Frame.encode(p));
            ch.position(0);
            for (byte[] expected : payloads) {
                byte[] got = Frame.decode(ch, MAX_PAYLOAD);
                assertThat(got).isEqualTo(expected);
            }
            // Fourth read is a clean EOF — not a torn frame.
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
        }
    }

    @Test
    void decode_at_clean_eof_returns_null_and_does_not_move_position(@TempDir Path dir)
            throws IOException {
        Path file = dir.resolve("empty.seg");
        Files.createFile(file);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            assertThat(ch.size()).isZero();
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            assertThat(ch.position()).isZero();
        }
    }

    @Test
    void decode_returns_null_when_length_prefix_is_torn(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("torn-header.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            // Two bytes: not enough for a 4-byte length prefix.
            ch.write(ByteBuffer.wrap(new byte[]{0x00, 0x05}));
            ch.position(0);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            // Position rewound to frame start so the caller can truncate.
            assertThat(ch.position()).isZero();
        }
    }

    @Test
    void decode_returns_null_when_payload_is_torn(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("torn-payload.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            // Declares 100 bytes of payload; writes only 5 bytes then EOF.
            ByteBuffer header = ByteBuffer.allocate(4);
            header.putInt(100).flip();
            ch.write(header);
            ch.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));
            ch.position(0);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            assertThat(ch.position()).isZero();
        }
    }

    @Test
    void decode_returns_null_when_crc_mismatches(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad-crc.seg");
        byte[] payload = "plausible content".getBytes();
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            ch.write(Frame.encode(payload));
            // Flip one bit in the payload region (after the 4-byte length prefix)
            // — the CRC at the end still references the ORIGINAL payload.
            ch.position(4);
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b);
            b.flip();
            byte flipped = (byte) (b.get() ^ 0x01);
            ch.position(4);
            ch.write(ByteBuffer.wrap(new byte[]{flipped}));
            ch.position(0);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            assertThat(ch.position()).isZero();
        }
    }

    @Test
    void decode_rejects_absurdly_large_length_declarations(@TempDir Path dir)
            throws IOException {
        Path file = dir.resolve("absurd-length.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            // Declares 100 MiB payload; maxPayload is 1 MiB → rejected
            // without ever attempting the allocation.
            ByteBuffer header = ByteBuffer.allocate(4);
            header.putInt(100 * 1024 * 1024).flip();
            ch.write(header);
            ch.position(0);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            assertThat(ch.position()).isZero();
        }
    }

    @Test
    void decode_rejects_negative_length_declarations(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("negative-length.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(4);
            header.putInt(-1).flip();
            ch.write(header);
            ch.position(0);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            assertThat(ch.position()).isZero();
        }
    }

    @Test
    void encode_rejects_null_payload() {
        assertThatThrownBy(() -> Frame.encode(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void empty_payload_is_a_valid_frame(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("empty-payload.seg");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            ch.write(Frame.encode(new byte[0]));
            ch.position(0);
            byte[] decoded = Frame.decode(ch, MAX_PAYLOAD);
            assertThat(decoded).isNotNull();
            assertThat(decoded).isEmpty();
        }
    }

    @Test
    void frame_after_a_torn_tail_is_unreadable_through_normal_scan(@TempDir Path dir)
            throws IOException {
        // Simulates a realistic crash scenario: two good frames, then a
        // torn one, then more "data" after it. The reader should stop at
        // the torn frame (even if the bytes past it happen to look like
        // a valid frame). This pins the torn-tail truncation semantic.
        Path file = dir.resolve("crash-sim.seg");
        byte[] good1 = "first".getBytes();
        byte[] good2 = "second".getBytes();
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ)) {
            ch.write(Frame.encode(good1));
            ch.write(Frame.encode(good2));
            // Torn frame: length header declaring 50 bytes, but only 3 bytes follow.
            ByteBuffer h = ByteBuffer.allocate(4);
            h.putInt(50).flip();
            ch.write(h);
            ch.write(ByteBuffer.wrap(new byte[]{'a', 'b', 'c'}));

            ch.position(0);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isEqualTo(good1);
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isEqualTo(good2);
            long beforeTorn = ch.position();
            assertThat(Frame.decode(ch, MAX_PAYLOAD)).isNull();
            // Position rewound to the start of the torn frame — the caller
            // truncates here.
            assertThat(ch.position()).isEqualTo(beforeTorn);
        }
    }
}
