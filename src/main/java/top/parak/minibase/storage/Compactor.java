package top.parak.minibase.storage;

import java.io.IOException;

/**
 * Abstract compactor thread.
 *
 * @author Khighness
 * @since 2023-08-03
 */
public abstract class Compactor extends Thread {

    /**
     * Compact files.
     *
     * @throws IOException if an IOException occurs
     */
    public abstract void compact() throws IOException;

}
