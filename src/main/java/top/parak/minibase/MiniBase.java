package top.parak.minibase;

import java.io.Closeable;
import java.io.IOException;

/**
 * Mini base.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public interface MiniBase extends Closeable {

    /**
     * Put a keyValue pair into MiniBase.
     *
     * @param key   the byte array of the key
     * @param value the byte array of the value
     * @throws IOException if an IOException occurs
     */
    void put(byte[] key, byte[] value) throws IOException;

    /**
     * Get the KeyValue corresponding to the specified key.
     *
     * @param key the byte array of the key
     * @return the KeyValue
     * @throws IOException if an IOException occurs
     */
    KeyValue get(byte[] key) throws IOException;

    /**
     * Delete the key-value pair corresponding to the specified key.
     *
     * @param key the byte array of the key
     * @throws IOException if an IOException occurs
     */
    void delete(byte[] key) throws IOException;

    /**
     * Fetch all the KeyValues whose key located in the range [startKey, stopKey)
     *
     * @param startKey the start key to scan (inclusive).
     *                 if startKey is {@code byte[0]}, it means negative infinity.
     * @param endKey   the end key to scan (exclusive).
     *                 if endKey is {@code byte[0]}, it means positive infinity.
     * @return Iterator for fetching KeyValue one by one
     * @throws IOException if an IOException occurs
     */
    Iter<KeyValue> scan(byte[] startKey, byte[] endKey) throws IOException;

}
