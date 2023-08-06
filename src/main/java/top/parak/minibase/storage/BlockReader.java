package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Block reader.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public class BlockReader {

    private final List<KeyValue> kvBuf;

    public BlockReader(List<KeyValue> kvBuf) {
        this.kvBuf = kvBuf;
    }

    public List<KeyValue> getKvBuf() {
        return kvBuf;
    }

    public static BlockReader deserialize(byte[] bytes, int offset, int size) throws IOException {
        int pos = offset;
        List<KeyValue> kvBuf = new ArrayList<>();
        Checksum crc32 = new CRC32();

        // Decode kv size
        int kvSize = Bytes.toInt(Bytes.slice(bytes, pos, BlockWriter.KV_SIZE_LEN));
        pos += BlockWriter.KV_SIZE_LEN;

        // Decode kv
        for (int i = 0; i < kvSize; i++) {
            KeyValue kv = KeyValue.deserialize(bytes, offset + pos);
            kvBuf.add(kv);
            crc32.update(bytes, offset + pos, kv.getSerializeSize());
            pos += kv.getSerializeSize();
        }

        // Decode checksum
        int checksum = Bytes.toInt(Bytes.slice(bytes, offset + pos, BlockWriter.CHECKSUM_LEN));
        pos += BlockWriter.CHECKSUM_LEN;

        int calChecksum = (int) (crc32.getValue() & 0xFFFFFFFF);
        if (calChecksum != checksum) {
            throw new IOException("checksum(" + checksum + ") is not equal to expected checksum(" + checksum + ")");
        }
        if (pos != size) {
            throw new IOException("pos(" + pos + ") should be equal to size(" + size + ")");
        }

        return new BlockReader(kvBuf);
    }

}
