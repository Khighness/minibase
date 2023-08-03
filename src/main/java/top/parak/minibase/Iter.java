package top.parak.minibase;

import java.io.IOException;

/**
 * Iterator.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public interface Iter<KV> {

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     * @throws IOException if an IOException occurs
     */
    boolean hasNext() throws IOException;

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws IOException if an IOException occurs
     */
    KV next() throws IOException;

}
