package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.Bytes;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Disk file writer.
 *
 * @author Khighness
 * @since 2023-08-04
 */
public class DiskFileWriter implements Closeable {

    public static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;

    /**
     * <ul>
     *     <li>{@link DiskFileWriter#fileSize}: 8B</li>
     *     <li>{@link DiskFileWriter#blockCount}: 4B</li>
     *     <li>{@link DiskFileWriter#blockIndexOffset}: 8B</li>
     *     <li>{@link DiskFileWriter#blockIndexSize}: 8B</li>
     *     <li>{@link DiskFileWriter#DISK_FILE_MAGIC}: 8B</li>
     * </ul>
     */
    public static final int  TRAILER_SIZE = 8 + 4 + 8 + 8 + 8;
    public static final long DISK_FILE_MAGIC = 0xC09111002L;

    private String           fileName;
    private FileOutputStream out;
    private long             currentOffset;
    private BlockIndexWriter indexWriter;
    private BlockWriter      currentWriter;

    private long             fileSize = 0;
    private int              blockCount = 0;
    private long             blockIndexOffset = 0;
    private long             blockIndexSize = 0;

    public DiskFileWriter(String fileName) throws IOException {
        this.fileName = fileName;

        File file = new File(this.fileName);
        file.createNewFile();

        out = new FileOutputStream(file, true);
        currentOffset = 0;
        indexWriter = new BlockIndexWriter();
        currentWriter = new BlockWriter();
    }

    private void switchNextBlockWriter() throws IOException {
        byte[] buf = currentWriter.serialize();
        out.write(buf);
        indexWriter.append(currentWriter.getLastKV(), currentOffset, buf.length, currentWriter.getBloomFilter());

        currentOffset += buf.length;
        blockCount += 1;

        currentWriter = new BlockWriter();
    }

    public void append(KeyValue kv) throws IOException {
        if (kv == null) {
            return;
        }

        int kvSize = kv.getSerializeSize() + BlockWriter.KV_SIZE_LEN + BlockWriter.CHECKSUM_LEN;
        if (kvSize >= BLOCK_SIZE_UP_LIMIT) {
            throw new IOException("KeyValue size(" + kvSize +  ") exceeds block limit(" + BLOCK_SIZE_UP_LIMIT + ")");
        }

        if (currentWriter.getKvCount() > 0
                && kv.getSerializeSize() + currentWriter.size() >= BLOCK_SIZE_UP_LIMIT) {
            switchNextBlockWriter();
        }

        currentWriter.append(kv);
    }

    public void appendIndex() throws IOException {
        if (currentWriter.getKvCount() > 0) {
            switchNextBlockWriter();
        }

        byte[] buf = indexWriter.serialize();
        blockIndexOffset = currentOffset;
        blockIndexSize = buf.length;

        out.write(buf);

        currentOffset += buf.length;
    }

    public void appendTrailer() throws IOException {
        fileSize = currentOffset + TRAILER_SIZE;

        // fileSize
        byte[] bytes = Bytes.toBytes(fileSize);
        out.write(bytes);

        // blockCount
        bytes = Bytes.toBytes(blockCount);
        out.write(bytes);

        // blockIndexOffset
        bytes = Bytes.toBytes(blockIndexOffset);
        out.write(bytes);

        // blockIndexSize
        bytes = Bytes.toBytes(blockIndexSize);
        out.write(bytes);

        // DISK_FILES_MAGIC
        bytes = Bytes.toBytes(DISK_FILE_MAGIC);
        out.write(bytes);
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            try {
                out.flush();
                FileDescriptor fd = out.getFD();
                fd.sync();
            } finally {
                out.close();
            }
        }
    }

}
