package top.parak.minibase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.parak.minibase.KeyValue;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Disk file.
 *
 * @author Khighness
 * @since 2023-08-02
 */
public class DiskFile implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DiskFile.class);
    private static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;

    public static final int  TRAILER_SIZE = 8 + 4 + 8 + 8 + 8;
    public static final long DIS_FILE_MAGIC = 0xC09111002L;

    private String fileName;
    private RandomAccessFile in;

    @Override
    public void close() throws IOException {

    }

    public static class BlockMeta implements Comparable<BlockMeta> {
        private static final int OFFSET_SIZE = 8;
        private static final int SIZE_SIZE = 8;
        private static final int BF_LEN_SIZE = 4;

        private final KeyValue lastKV;
        private final long blockOffset;
        private final long blockSize;
        private final byte[] bloomFilter;

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
    }

}
