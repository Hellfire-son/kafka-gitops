// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops;

import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Factory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.xerial.snappy.Snappy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Local codec and JNI checks; never constructs a Kafka client or opens a connection. */
class CompressionCompatibilityTest {
    private static List<byte[]> payloads() {
        byte[] repeated = new byte[1024 * 1024];
        Arrays.fill(repeated, (byte) 37);
        byte[] random = new byte[repeated.length];
        new Random(431392L).nextBytes(random);
        return List.of(new byte[0], "Kafka: Привет".getBytes(StandardCharsets.UTF_8),
                new byte[1], new byte[1024], new byte[65536], repeated, random);
    }

    static Stream<Arguments> recordCases() {
        return Arrays.stream(CompressionType.values()).flatMap(codec ->
                payloads().stream().map(payload -> Arguments.of(codec, payload)));
    }

    static Stream<Arguments> nativeCases() {
        return Stream.of(CompressionType.LZ4, CompressionType.SNAPPY, CompressionType.ZSTD)
                .flatMap(codec -> payloads().stream().map(payload -> Arguments.of(codec, payload)));
    }

    @ParameterizedTest(name = "Kafka record batch {0}, payload case {index}")
    @MethodSource("recordCases")
    void kafkaRecordBatchesRoundTrip(CompressionType codec, byte[] payload) {
        MemoryRecords records = MemoryRecords.withRecords(Compression.of(codec).build(),
                new SimpleRecord(0L, new byte[]{1}, payload), new SimpleRecord(1L, new byte[]{2}, payload));
        int count = 0;
        for (var batch : records.batches()) {
            batch.ensureValid();
            assertEquals(codec, batch.compressionType());
            for (var record : batch) {
                byte[] actual = new byte[record.valueSize()];
                record.value().get(actual);
                assertArrayEquals(payload, actual);
                count++;
            }
        }
        assertEquals(2, count);
    }

    @ParameterizedTest(name = "JNI arrays and direct buffers {0}, payload case {index}")
    @MethodSource("nativeCases")
    void nativeArraysAndDirectBuffersRoundTrip(CompressionType codec, byte[] payload) throws Exception {
        ByteBuffer source = ByteBuffer.allocateDirect(payload.length).put(payload).flip();
        ByteBuffer encoded;
        ByteBuffer decoded = ByteBuffer.allocateDirect(payload.length);
        int restored;
        switch (codec) {
            case LZ4 -> {
                var factory = LZ4Factory.nativeInstance();
                assertTrue(factory.fastCompressor().getClass().getSimpleName().contains("JNI"));
                byte[] bytes = factory.fastCompressor().compress(payload);
                assertArrayEquals(payload, factory.safeDecompressor().decompress(bytes, payload.length));
                encoded = ByteBuffer.allocateDirect(factory.fastCompressor().maxCompressedLength(payload.length));
                int size = factory.fastCompressor().compress(source, 0, payload.length, encoded, 0, encoded.capacity());
                restored = factory.safeDecompressor().decompress(encoded, 0, size, decoded, 0, decoded.capacity());
            }
            case SNAPPY -> {
                assertArrayEquals(payload, Snappy.uncompress(Snappy.compress(payload)));
                encoded = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(payload.length));
                int size = Snappy.compress(source, encoded);
                encoded.position(0).limit(size);
                restored = Snappy.uncompress(encoded, decoded);
            }
            case ZSTD -> {
                assertArrayEquals(payload, Zstd.decompress(Zstd.compress(payload), payload.length));
                encoded = ByteBuffer.allocateDirect((int) Zstd.compressBound(payload.length));
                long size = Zstd.compressDirectByteBuffer(encoded, 0, encoded.capacity(), source, 0, payload.length, 3);
                assertFalse(Zstd.isError(size));
                long result = Zstd.decompressDirectByteBuffer(decoded, 0, decoded.capacity(), encoded, 0, (int) size);
                assertFalse(Zstd.isError(result));
                restored = (int) result;
            }
            default -> throw new AssertionError(codec);
        }
        assertEquals(payload.length, restored);
        byte[] actual = new byte[restored];
        decoded.position(0).limit(restored);
        decoded.get(actual);
        assertArrayEquals(payload, actual);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"LZ4", "SNAPPY", "ZSTD"})
    void malformedPayloadIsRejected(CompressionType codec) {
        byte[] malformed = {(byte) 0xff};
        switch (codec) {
            case LZ4 -> assertThrows(RuntimeException.class,
                    () -> LZ4Factory.safeInstance().safeDecompressor().decompress(malformed, 16));
            case SNAPPY -> assertThrows(Exception.class, () -> Snappy.uncompress(malformed));
            case ZSTD -> assertThrows(RuntimeException.class, () -> Zstd.decompress(malformed, 16));
            default -> throw new AssertionError(codec);
        }
    }
}
