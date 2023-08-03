package top.parak.minibase.storage;

import top.parak.minibase.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Block index writer.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public class BlockIndexWriter {

    private final List<BlockMeta> blockMetaList = new ArrayList<>();
    private int totalBytes = 0;

    public void append(KeyValue lastKV, long offset, long size, byte[] bloomFilter) {
        BlockMeta blockMeta = new BlockMeta(lastKV, offset, size, bloomFilter);
        blockMetaList.add(blockMeta);
        totalBytes += blockMeta.getSerializeSize();
    }

    public byte[] serialize() throws IOException {
        byte[] buffer = new byte[totalBytes];
        int pos = 0;
        for (BlockMeta blockMeta : blockMetaList) {
            byte[] metaBytes = blockMeta.serialize();
            System.arraycopy(metaBytes, 0, buffer, pos, metaBytes.length);
            pos += blockMeta.getSerializeSize();
        }
        return buffer;
    }

}
