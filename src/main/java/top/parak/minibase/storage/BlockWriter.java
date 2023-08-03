package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.BloomFilter;
import top.parak.minibase.toolkit.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Block writer.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public class BlockWriter {

    // QAZ
    // WSX
    // EDC
    // RFV TGB
    // YHN UJM
    // IK,
    // OL.
    // P;/

    private static final int BLOOM_FILTER_HASH_COUNT = 3;
    private static final int BLOOM_FILTER_BITS_PER_KEY = 10;

    public static final int KV_SIZE_LEN = 4;
    public static final int CHECKSUM_LEN = 4;

    private int            totalSize;
    private List<KeyValue> kvBuf;
    private BloomFilter    bloomFilter;
    private Checksum       crc32;
    private KeyValue       lastKV;
    private int            kvCount;

    public BlockWriter() {
        totalSize = 0;
        kvBuf = new ArrayList<>();
        bloomFilter = new BloomFilter(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_BITS_PER_KEY);
        crc32 = new CRC32();
    }

    public int getTotalSize() {
        return totalSize;
    }

    public List<KeyValue> getKvBuf() {
        return kvBuf;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public int getChecksum() {
        return (int) crc32.getValue();
    }

    public KeyValue getLastKV() {
        return lastKV;
    }

    public int getKvCount() {
        return kvCount;
    }

    public int size() {
        return KV_SIZE_LEN + totalSize + CHECKSUM_LEN;
    }

    public void append(KeyValue kv) throws IOException {
        // Update key value buffer
        kvBuf.add(kv);
        lastKV = kv;

        // Update checksum
        byte[] buf = kv.serialize();
        crc32.update(buf, 0, buf.length);

        totalSize += kv.getSerializeSize();
        kvCount++;
    }

    public byte[] serialize() throws IOException {
        byte[] bytes = new byte[size()];
        int pos = 0;

        // Append kv size
        byte[] kvSize = Bytes.toBytes(kvBuf.size());
        System.arraycopy(kvSize, 0, bytes, pos, kvSize.length);
        pos += kvSize.length;

        // Append kv
        for (KeyValue kv : kvBuf) {
            byte[] kvBytes = kv.serialize();
            System.arraycopy(kvBytes, 0, bytes, pos, kvBytes.length);
            pos += kvBytes.length;
        }

        // Append checksum
        byte[] checkSum = Bytes.toBytes(getChecksum());
        System.arraycopy(checkSum, 0, bytes, pos, checkSum.length);
        pos += checkSum.length;

        if (pos != bytes.length) {
            throw new IOException("pos(" + pos + ") should be equal to length of bytes(" + bytes.length + ")");
        }
        return bytes;
    }

}
