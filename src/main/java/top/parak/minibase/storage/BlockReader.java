package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public static BlockReader serializeFrom(byte[] buffer, int offset, int size) throws IOException {
        int pos = offset;
        List<KeyValue> kvBuf = new ArrayList<>();

        // Decode kv size
        int kvSize = Bytes.toInt(Bytes.slice(buffer, pos, BlockWriter.KV_SIZE_LEN));

        return null;
    }

}
