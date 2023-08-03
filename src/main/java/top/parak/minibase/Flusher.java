package top.parak.minibase;

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
     * @throws Exception if an Exception occurs
     */
    void flush(Iter<KeyValue> it) throws Exception;

}
