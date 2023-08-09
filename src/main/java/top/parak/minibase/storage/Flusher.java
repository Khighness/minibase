package top.parak.minibase.storage;

import top.parak.minibase.Iter;
import top.parak.minibase.KeyValue;

import java.io.IOException;

/**
 * Flusher.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public interface Flusher {

    /**
     * Flush file.
     *
     * @param it iterator for fetching KeyValue one by one
     * @throws IOException if an IOException occurs
     */
    void flush(Iter<KeyValue> it) throws IOException;

}
