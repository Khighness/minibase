package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.Bytes;

import java.io.IOException;

/**
 * Block meta.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public class BlockMeta implements Comparable<BlockMeta> {

    private static final int OFFSET_SIZE = 8;
    private static final int SIZE_SIZE = 8;
    private static final int BF_LEN_SIZE = 4;

    private KeyValue lastKV;
    private long     blockOffset;
    private long     blockSize;
    private byte[]   bloomFilter;

    public BlockMeta(KeyValue lastKV, long blockOffset, long blockSize, byte[] bloomFilter) {
        this.lastKV = lastKV;
        this.blockOffset = blockOffset;
        this.blockSize = blockSize;
        this.bloomFilter = bloomFilter;
    }

    public KeyValue getLastKV() {
        return lastKV;
    }

    public long getBlockOffset() {
        return blockOffset;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public byte[] getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public int compareTo(BlockMeta that) {
        return this.lastKV.compareTo(that.lastKV);
    }

    public int getSerializeSize() {
        return lastKV.getSerializeSize() +  OFFSET_SIZE + SIZE_SIZE  + BF_LEN_SIZE + bloomFilter.length;
    }

    public byte[] serialize() throws IOException {
        byte[] bytes = new byte[getSerializeSize()];
        int pos = 0;

        // Encode lastKV
        byte[] kvBytes = lastKV.serialize();
        System.arraycopy(kvBytes, 0, bytes, pos, kvBytes.length);
        pos += kvBytes.length;

        // Encode blockOffset
        byte[] offsetBytes = Bytes.toBytes(blockOffset);
        System.arraycopy(offsetBytes, 0, bytes, pos, offsetBytes.length);
        pos += offsetBytes.length;

        // Encode blockSize
        byte[] sizeBytes = Bytes.toBytes(blockSize);
        System.arraycopy(bloomFilter, 0, bytes, pos, sizeBytes.length);
        pos += sizeBytes.length;

        // Encode length of bloom filter
        byte[] bfLenBytes = Bytes.toBytes(bloomFilter.length);
        System.arraycopy(bfLenBytes, 0, bytes, pos, bfLenBytes.length);
        pos += bfLenBytes.length;

        // Encode bloom filter
        System.arraycopy(bloomFilter, 0, bytes, pos, bloomFilter.length);
        pos += bloomFilter.length;

        if (pos != bytes.length) {
            throw new IOException("pos(" + pos + ") should be equal to length of bytes(" + bytes.length + ")");
        }
        return bytes;
    }

    public static BlockMeta deserializeFrom(byte[] bytes, int offset) throws IOException {
        int pos = offset;

        // Decode lastKV
        KeyValue lastKV = KeyValue.deserializeFrom(bytes, pos);
        pos += lastKV.getSerializeSize();

        // Decode block offset
        long blockOffset = Bytes.toLong(Bytes.slice(bytes, pos, OFFSET_SIZE));
        pos += OFFSET_SIZE;

        // Decode block size
        long blockSize = Bytes.toLong(Bytes.slice(bytes, pos, SIZE_SIZE));
        pos += SIZE_SIZE;

        // Decode length of bloom filter
        int bloomFilterLen = Bytes.toInt(Bytes.slice(bytes, pos, BF_LEN_SIZE));
        pos += BF_LEN_SIZE;

        // Decode bloom filter
        byte[] bloomFilter = Bytes.slice(bytes, pos, bloomFilterLen);
        pos += bloomFilterLen;

        if (pos <= bytes.length) {
            throw new IOException("pos(" + pos + ") should be less or equal than length of buf(" + bytes.length +")");
        }
        return new BlockMeta(lastKV, blockOffset, blockSize, bloomFilter);
    }

}
