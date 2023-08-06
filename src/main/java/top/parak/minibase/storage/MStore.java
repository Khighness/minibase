package top.parak.minibase.storage;

import top.parak.minibase.Compactor;
import top.parak.minibase.Iter;
import top.parak.minibase.KeyValue;
import top.parak.minibase.MiniBase;
import top.parak.minibase.config.Config;
import top.parak.minibase.toolkit.Requires;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MStore.
 *
 * @author Khighness
 * @since 2023-08-06
 */
public class MStore implements MiniBase {

    private ExecutorService executorService;
    private DiskStore       diskStore;
    private Compactor       compactor;
    private AtomicLong      sequenceId;
    private Config          config;

    public static MStore create(Config config) {
        Requires.requireNotNull(config);
        return new MStore(config);
    }

    private MStore(Config config) {
        this.config = config;
    }

    public MiniBase open() throws Exception {
        AtomicInteger threadCounter = new AtomicInteger(0);
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r);
            t.setName(String.format("mstore-%d", threadCounter.incrementAndGet()));
            return t;
        };
        this.executorService = new ThreadPoolExecutor(
                config.getMaxThreadPoolSize(),
                config.getMaxThreadPoolSize(),
                0, TimeUnit.MINUTES,
                new SynchronousQueue<>(),
                threadFactory
        );

        this.diskStore = new DiskStore(config.getDataDir(), config.getMaxDiskFiles());
        this.diskStore.open();
        this.sequenceId = new AtomicLong(0);
        this.compactor = new DiskFileCompactor(diskStore);
        this.compactor.start();
        return this;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {

    }

    @Override
    public KeyValue get(byte[] key) throws IOException {
        return null;
    }

    @Override
    public void delete(byte[] key) throws IOException {

    }

    @Override
    public Iter<KeyValue> scan(byte[] startKey, byte[] endKey) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

}
