package top.parak.minibase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.parak.minibase.Flusher;
import top.parak.minibase.KeyValue;
import top.parak.minibase.SeekIter;
import top.parak.minibase.config.Config;
import top.parak.minibase.storage.DiskStore.MultiIter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Memory store.
 *
 * @author Khighness
 * @since 2023-08-08
 */
public class MemStore implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MemStore.class);

    private final AtomicLong             dataSize = new AtomicLong();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean          isSnapshotFlushing = new AtomicBoolean(false);

    private volatile ConcurrentSkipListMap<KeyValue, KeyValue> kvMap;
    private volatile ConcurrentSkipListMap<KeyValue, KeyValue> snapshot;

    private Config          config;
    private Flusher         flusher;
    private ExecutorService executorService;

    public MemStore(Config config, Flusher flusher, ExecutorService executorService, ) {
        this.config = config;
        this.flusher = flusher;
        this.executorService = executorService;

        this.dataSize.set(0);
        this.kvMap = new ConcurrentSkipListMap<>();
        this.snapshot = null;
    }

    public void add(KeyValue kv) throws IOException {
        flushIfNeeded(true);
        lock.readLock().lock();
        try {
            KeyValue prevKv;
            if ((prevKv = kvMap.put(kv, kv)) == null) {
                dataSize.addAndGet(kv.getSerializeSize());
            } else {
                dataSize.addAndGet(kv.getSerializeSize() - prevKv.getSerializeSize());
            }
        } finally {
            lock.readLock().unlock();
        }
        flushIfNeeded(false);
    }

    private void flushIfNeeded(boolean shouldBlocking) throws IOException {
        if (getDataSize() > config.getMaxMemStoreSize()) {
            if (isSnapshotFlushing.get() && shouldBlocking) {
                throw new IOException("MemStore is full, current data size is " + dataSize.get() + "B, max is "
                        + config.getMaxMemStoreSize() + "B, please wait util the the flushing is finished.");
            } else if (isSnapshotFlushing.compareAndSet(false, true)) {
                executorService.submit();
            }
        }
    }

    public long getDataSize() {
        return this.dataSize.get();
    }

    public boolean isFlushing() {
        return this.isSnapshotFlushing.get();
    }

    @Override
    public void close() throws IOException {
    }

    private class FlusherTask implements Runnable {
        @Override
        public void run() {
            // 1. Save snapshot.
            lock.writeLock().lock();
            try {
                snapshot = kvMap;
                kvMap = new ConcurrentSkipListMap<>();
                dataSize.set(0);
            } finally {
                lock.writeLock().unlock();
            }

            // 2. Flush snapshot to disk file.
            boolean success = false;
            for (int i = 0; i < config.getMaxFlushRetries(); i++) {
                try {
                    flusher.flush(new IteratorWrapper(snapshot));
                    success = true;
                } catch (IOException e) {
                    LOG.error("Flush failed, retry times is " + i + ", max is " + config.getMaxFlushRetries());
                    if (i >= config.getMaxFlushRetries()) {
                        break;
                    }
                }
            }

            // 3. Clear the snapshot.
            if (success) {
                snapshot = null;
                isSnapshotFlushing.compareAndSet(true, false);
            }
        }
    }

    public static class IteratorWrapper implements SeekIter<KeyValue> {
        private SortedMap<KeyValue, KeyValue> sortedMap;
        private Iterator<KeyValue> it;

        public IteratorWrapper(SortedMap<KeyValue, KeyValue> sortedMap) {
            this.sortedMap = sortedMap;
            this.it = sortedMap.values().iterator();
        }

        @Override
        public boolean hasNext() throws IOException {
            return it != null && it.hasNext();
        }

        @Override
        public KeyValue next() throws IOException {
            return it.next();
        }

        @Override
        public void seekTo(KeyValue target) throws IOException {
            it = sortedMap.tailMap(target).values().iterator();
        }
    }

    public class MemStoreIter implements SeekIter<KeyValue> {
        private MultiIter it;

        public MemStoreIter(NavigableMap<KeyValue, KeyValue> kvSet,
                            NavigableMap<KeyValue, KeyValue> snapshot) throws IOException {
            List<IteratorWrapper> inputs = new ArrayList<>();
            if (kvSet != null && kvSet.size() > 0) {
                inputs.add(new IteratorWrapper(kvMap));
            }
            if (snapshot != null && snapshot.size() > 0) {
                inputs.add(new IteratorWrapper(snapshot));
            }
            it = new MultiIter(inputs.toArray(new IteratorWrapper[0]));
        }

        @Override
        public boolean hasNext() throws IOException {
            return it.hasNext();
        }

        @Override
        public KeyValue next() throws IOException {
            return it.next();
        }

        @Override
        public void seekTo(KeyValue target) throws IOException {
            it.seekTo(target);
        }
    }

}
