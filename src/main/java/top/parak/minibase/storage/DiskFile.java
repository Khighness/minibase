package top.parak.minibase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.parak.minibase.toolkit.Bytes;
import top.parak.minibase.toolkit.Requires;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.SortedSet;

/**
 * Disk file.
 *
 * @author Khighness
 * @since 2023-08-02
 */
public class DiskFile implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DiskFile.class);

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
            BlockMeta blockMeta = BlockMeta.deserializeFrom(bytes, offset);
            blockMetaSet.add(blockMeta);
            offset += blockMeta.getSerializeSize();
        } while (offset < bytes.length);

        Requires.requireTrue(blockMetaSet.size() == blockCount);
    }

    @Override
    public void close() throws IOException {

    }

}
