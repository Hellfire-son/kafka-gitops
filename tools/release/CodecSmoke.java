// SPDX-License-Identifier: Apache-2.0
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Factory;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.xerial.snappy.Snappy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

class CodecSmoke {
    private static int arrayChecks, directChecks, kafkaChecks;

    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError(reason);
    }

    private static List<byte[]> payloads() {
        List<byte[]> result = new ArrayList<>();
        result.add(new byte[0]);
        result.add("Kafka codec synthetic data: \u041f\u0440\u0438\u0432\u0435\u0442".getBytes(StandardCharsets.UTF_8));
        Random random = new Random(431392L);
        for (int size : new int[]{1, 1024, 65536, 131072, 1048576}) {
            byte[] repeated = new byte[size];
            Arrays.fill(repeated, (byte) 37);
            result.add(repeated);
            byte[] bytes = new byte[size];
            random.nextBytes(bytes);
            result.add(bytes);
        }
        return result;
    }

    private static ByteBuffer direct(byte[] payload) {
        return ByteBuffer.allocateDirect(payload.length).put(payload).flip();
    }

    private static void checkBuffer(ByteBuffer buffer, int length, byte[] expected) {
        buffer.position(0).limit(length);
        byte[] actual = new byte[length];
        buffer.get(actual);
        require(Arrays.equals(expected, actual), "Direct-buffer roundtrip mismatch");
    }

    private static void kafka(CompressionType type, byte[] payload) {
        MemoryRecords records = MemoryRecords.withRecords(Compression.of(type).build(),
                new SimpleRecord(0L, new byte[]{1}, payload),
                new SimpleRecord(1L, new byte[]{2}, payload),
                new SimpleRecord(2L, new byte[]{3}, payload));
        int count = 0;
        for (RecordBatch batch : records.batches()) {
            batch.ensureValid();
            require(batch.magic() == RecordBatch.CURRENT_MAGIC_VALUE, "Kafka record magic");
            require(batch.compressionType() == type, "Kafka compression type");
            for (org.apache.kafka.common.record.internal.Record record : batch) {
                ByteBuffer value = record.value();
                byte[] actual = new byte[value.remaining()];
                value.get(actual);
                require(Arrays.equals(payload, actual), "Kafka record roundtrip mismatch");
                count++;
            }
        }
        require(count == 3, "Kafka record count");
        kafkaChecks++;
    }

    private static void lz4(List<byte[]> inputs) {
        LZ4Factory[] factories = {LZ4Factory.safeInstance(), LZ4Factory.fastestJavaInstance(), LZ4Factory.nativeInstance()};
        require(factories[2].fastCompressor().getClass().getSimpleName().contains("JNI"), "LZ4 native compressor not loaded");
        require(factories[2].safeDecompressor().getClass().getSimpleName().contains("JNI"), "LZ4 native decompressor not loaded");
        for (LZ4Factory factory : factories) {
            for (byte[] payload : inputs) {
                byte[] compressed = factory.fastCompressor().compress(payload);
                require(Arrays.equals(payload, factory.safeDecompressor().decompress(compressed, payload.length)), "LZ4 array roundtrip");
                arrayChecks++;
                ByteBuffer source = direct(payload);
                ByteBuffer encoded = ByteBuffer.allocateDirect(factory.fastCompressor().maxCompressedLength(payload.length));
                int size = factory.fastCompressor().compress(source, 0, payload.length, encoded, 0, encoded.capacity());
                ByteBuffer decoded = ByteBuffer.allocateDirect(payload.length);
                int restored = factory.safeDecompressor().decompress(encoded, 0, size, decoded, 0, decoded.capacity());
                checkBuffer(decoded, restored, payload);
                directChecks++;
            }
        }
        for (byte[] payload : inputs) kafka(CompressionType.LZ4, payload);
        System.out.println("LZ4 native backends: " + factories[2].fastCompressor().getClass().getName() + ", " + factories[2].safeDecompressor().getClass().getName());
    }

    private static void snappy(List<byte[]> inputs) throws Exception {
        for (byte[] payload : inputs) {
            require(Arrays.equals(payload, Snappy.uncompress(Snappy.compress(payload))), "Snappy array roundtrip");
            arrayChecks++;
            ByteBuffer source = direct(payload);
            ByteBuffer encoded = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(payload.length));
            int size = Snappy.compress(source, encoded);
            encoded.position(0).limit(size);
            ByteBuffer decoded = ByteBuffer.allocateDirect(payload.length);
            int restored = Snappy.uncompress(encoded, decoded);
            checkBuffer(decoded, restored, payload);
            directChecks++;
            kafka(CompressionType.SNAPPY, payload);
        }
        System.out.println("Snappy native library version: " + Snappy.getNativeLibraryVersion());
    }

    private static void zstd(List<byte[]> inputs) {
        for (byte[] payload : inputs) {
            require(Arrays.equals(payload, Zstd.decompress(Zstd.compress(payload), payload.length)), "Zstd array roundtrip");
            arrayChecks++;
            ByteBuffer source = direct(payload);
            ByteBuffer encoded = ByteBuffer.allocateDirect((int) Zstd.compressBound(payload.length));
            long size = Zstd.compressDirectByteBuffer(encoded, 0, encoded.capacity(), source, 0, payload.length, 3);
            require(!Zstd.isError(size), "Zstd direct compression error");
            ByteBuffer decoded = ByteBuffer.allocateDirect(payload.length);
            long restored = Zstd.decompressDirectByteBuffer(decoded, 0, decoded.capacity(), encoded, 0, (int) size);
            require(!Zstd.isError(restored), "Zstd direct decompression error");
            checkBuffer(decoded, (int) restored, payload);
            directChecks++;
            kafka(CompressionType.ZSTD, payload);
        }
        System.out.println("Zstd JNI arrays and direct buffers loaded successfully");
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "all" : args[0];
        List<byte[]> inputs = payloads();
        switch (mode) {
            case "lz4": lz4(inputs); break;
            case "snappy": snappy(inputs); break;
            case "zstd": zstd(inputs); break;
            case "all": lz4(inputs); snappy(inputs); zstd(inputs); break;
            default: throw new IllegalArgumentException(mode);
        }
        System.out.printf("PASS mode=%s payloads=%d array=%d direct=%d KafkaRecordBatches=%d%n",
                mode, inputs.size(), arrayChecks, directChecks, kafkaChecks);
    }
}
