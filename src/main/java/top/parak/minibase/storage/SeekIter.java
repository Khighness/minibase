package top.parak.minibase.storage;

import top.parak.minibase.Iter;

import java.io.IOException;

/**
 * Seek iterator.
 *
 * @author Khighness
 * @since 2023-08-04
 */
public interface SeekIter<KeyValue> extends Iter<KeyValue> {

    /**
     * Seek to the smallest KeyValue which is greater than the given value.
     *
      * @param target the specified KeyValue
     * @throws IOException if an IOException occurs
     */
    void seekTo(KeyValue target) throws IOException;

}
