package top.parak.minibase;

import top.parak.minibase.config.Config;
import top.parak.minibase.storage.Compactor;
import top.parak.minibase.storage.DiskStoreCompactor;
import top.parak.minibase.storage.DiskStoreFlusher;
import top.parak.minibase.storage.DiskStore;
import top.parak.minibase.storage.DiskStore.MultiIter;
import top.parak.minibase.storage.MemStore;
import top.parak.minibase.storage.SeekIter;
import top.parak.minibase.toolkit.Bytes;
import top.parak.minibase.toolkit.Requires;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    private DiskStore diskStore;
    private MemStore memStore;
    private Compactor compactor;
    private AtomicLong      sequenceId;
    private Config          config;

    public static MStore create(Config config) {
        Requires.requireNotNull(config, "");
        return new MStore(config);
    }

    public static MStore create() {
        return new MStore(Config.getDefault());
    }

    private MStore(Config config) {
        this.config = config;
    }

    public MiniBase open() throws IOException {
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
        this.memStore = new MemStore(config, new DiskStoreFlusher(diskStore), executorService);
        this.sequenceId = new AtomicLong(0);
        this.compactor = new DiskStoreCompactor(diskStore);
        this.compactor.start();
        return this;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        this.memStore.add(KeyValue.createPut(key, value, sequenceId.incrementAndGet()));
    }

    @Override
    public KeyValue get(byte[] key) throws IOException {
        KeyValue result = null;
        Iter<KeyValue> it = scan(key, Bytes.EMPTY_BYTES);
        if (it.hasNext()) {
            KeyValue kv = it.next();
            if (Bytes.compare(kv.getKey(), key) == 0) {
                result = kv;
            }
        }
        return result;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        this.memStore.add(KeyValue.createDelete(key, sequenceId.incrementAndGet()));
    }

    @Override
    public Iter<KeyValue> scan(byte[] startKey, byte[] endKey) throws IOException {
        List<SeekIter<KeyValue>> iterList = new ArrayList<>();
        iterList.add(memStore.createIterator());
        iterList.add(diskStore.createIterator());
        MultiIter it = new MultiIter(iterList);

        if (Bytes.compare(startKey, Bytes.EMPTY_BYTES) != 0) {
            it.seekTo(KeyValue.createDelete(startKey, sequenceId.get()));
        }

        KeyValue stopKV = null;
        if (Bytes.compare(endKey, Bytes.EMPTY_BYTES) != 0) {
            stopKV = KeyValue.createDelete(endKey, Long.MAX_VALUE);
        }
        return new ScanIter(stopKV, it);
    }

    @Override
    public void close() throws IOException {
        memStore.close();
        diskStore.close();
        compactor.interrupt();
    }

    static class ScanIter implements Iter<KeyValue> {
        private KeyValue stopKV;
        private Iter<KeyValue> storeIt;
        private KeyValue lastKV    = null;
        private KeyValue pendingKV = null;

        public ScanIter(KeyValue stopKV, SeekIter<KeyValue> it) {
            this.stopKV = stopKV;
            this.storeIt = it;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (pendingKV == null) {
                switchToNextKey();
            }
            return pendingKV != null;
        }

        @Override
        public KeyValue next() throws IOException {
            if (pendingKV == null) {
                switchToNextKey();
            }
            lastKV = pendingKV;
            pendingKV = null;
            return lastKV;
        }

        private boolean shouldStop(KeyValue kv) {
            return stopKV != null && Bytes.compare(stopKV.getKey(), kv.getKey()) <= 0;
        }

        private void switchToNextKey() throws IOException {
            if (lastKV != null && shouldStop(lastKV)) {
                return;
            }
            KeyValue currKV;
            while (storeIt.hasNext()) {
                currKV = storeIt.next();
                if (shouldStop(currKV)) {
                    return;
                }
                if (currKV.getOp() == Op.Put) {
                    if (lastKV == null) {
                        lastKV = pendingKV = currKV;
                        return;
                    }
                    int ret = Bytes.compare(lastKV.getKey(), currKV.getKey());
                    if (ret < 0) {
                        lastKV = pendingKV = currKV;
                        return;
                    } else if (ret > 0) {
                        throw new IOException("KV mis-encoded, currKV < lastKV, currKV: " + Bytes.toHex(currKV.getKey())
                                + ", lastKV: " + Bytes.toHex(lastKV.getKey()));
                    }
                } else if (currKV.getOp() == Op.Delete) {
                    if (lastKV == null || Bytes.compare(lastKV.getKey(), currKV.getKey()) != 0) {
                        lastKV = currKV;
                    }
                } else {
                    throw new IllegalStateException("Unknown op code: " + currKV.getOp());
                }
            }
        }
    }

}
