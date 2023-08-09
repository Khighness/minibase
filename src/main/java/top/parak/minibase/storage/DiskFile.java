package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.Bytes;
import top.parak.minibase.toolkit.Requires;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * Disk file.
 *
 * @author Khighness
 * @since 2023-08-02
 */
public class DiskFile implements Closeable {

    private String               fileName;
    private RandomAccessFile     in;
    private SortedSet<BlockMeta> blockMetaSet;

    private long fileSize;
    private int  blockCount;
    private long blockIndexOffset;
    private long blockIndexSize;

    public void open(String fileName) throws IOException {
        this.fileName = fileName;

        File file = new File(fileName);
        in = new RandomAccessFile(file, "r");

        fileSize = file.length();
        Requires.requireTrue(fileSize > DiskFileWriter.TRAILER_SIZE);
        in.seek(fileSize - DiskFileWriter.TRAILER_SIZE);

        byte[] bytes = new byte[8];
        Requires.requireTrue(in.read(bytes) == bytes.length);
        blockCount = Bytes.toInt(bytes);

        bytes = new byte[4];
        Requires.requireTrue(in.read(bytes) == bytes.length);
        blockIndexOffset = Bytes.toInt(bytes);

        bytes = new byte[8];
        Requires.requireTrue(in.read(bytes) == bytes.length);
        blockIndexSize = Bytes.toLong(bytes);

        bytes = new byte[8];
        Requires.requireTrue(in.read(bytes) == bytes.length);
        Requires.requireTrue(DiskFileWriter.DISK_FILE_MAGIC == Bytes.toLong(bytes));

        bytes = new byte[(int) blockIndexSize];
        in.seek(blockIndexOffset);
        Requires.requireTrue(in.read(bytes) == blockIndexSize);

        int offset = 0;
        do {
            BlockMeta blockMeta = BlockMeta.deserialize(bytes, offset);
            blockMetaSet.add(blockMeta);
            offset += blockMeta.getSerializeSize();
        } while (offset < bytes.length);

        Requires.requireTrue(blockMetaSet.size() == blockCount);
    }

    public String getFileName() {
        return fileName;
    }

    private BlockReader load(BlockMeta meta) throws IOException {
        in.seek(meta.getBlockOffset());

        byte[] bytes = new byte[(int) meta.getBlockSize()];
        Requires.requireTrue(in.read(bytes) == bytes.length);
        return BlockReader.deserialize(bytes, 0, bytes.length);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    public SeekIter<KeyValue> iterator() {
        return new InternalSeekIterator();
    }

    private class InternalSeekIterator implements SeekIter<KeyValue> {

        private int currentKVIndex = 0;
        private BlockReader currentReader;
        private Iterator<BlockMeta> blockMetaIter;

        public InternalSeekIterator() {
            this.currentReader = null;
            this.blockMetaIter = blockMetaSet.iterator();
        }

        private boolean nextBlockReader() throws IOException {
            if (blockMetaIter.hasNext()) {
                currentReader = load(blockMetaIter.next());
                currentKVIndex = 0;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean hasNext() throws IOException {
            if (currentReader == null) {
                return nextBlockReader();
            } else {
                if (currentKVIndex < currentReader.getKvBuf().size()) {
                    return true;
                } else {
                    return nextBlockReader();
                }
            }
        }

        @Override
        public KeyValue next() throws IOException {
            return currentReader.getKvBuf().get(currentKVIndex++);
        }

        @Override
        public void seekTo(KeyValue target) throws IOException {
            blockMetaIter = blockMetaSet.tailSet(new BlockMeta(target, 0, 0, Bytes.EMPTY_BYTES)).iterator();
            currentReader = null;
            if (blockMetaIter.hasNext()) {
                BlockReader reader = load(blockMetaIter.next());
                currentKVIndex = 0;
                while (currentKVIndex < currentReader.getKvBuf().size()) {
                    KeyValue currKV = currentReader.getKvBuf().get(currentKVIndex);
                    if (currKV.compareTo(target) >= 0) {
                        break;
                    }
                    currentKVIndex++;
                }
                if (currentKVIndex >= currentReader.getKvBuf().size()) {
                    throw new IOException("Data block mis-encoded, lastKV of the currentReader >= kv, but " +
                            "we found all kv < target");
                }
            }
        }

    }

}
