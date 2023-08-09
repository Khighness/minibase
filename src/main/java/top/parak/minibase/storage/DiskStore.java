package top.parak.minibase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.parak.minibase.KeyValue;
import top.parak.minibase.toolkit.Requires;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Disk store.
 *
 * @author Khighness
 * @since 2023-08-05
 */
public class DiskStore implements Closeable {

    public static final String FILE_NAME_TMP_SUFFIX = ".tmp";
    public static final String FILE_NAME_ARCHIVE_SUFFIX = ".archive";

    private static final Logger  LOG = LoggerFactory.getLogger(DiskStore.class);
    private static final Pattern DATA_FILE_RE = Pattern.compile("data\\.([0-9]+)");

    private String         dataDir;
    private List<DiskFile> diskFiles;

    private int                 maxDiskFiles;
    private volatile AtomicLong maxFileId;

    public DiskStore(String dataDir, int maxDiskFiles) {
        this.dataDir = dataDir;
        this.diskFiles = new ArrayList<>();
        this.maxDiskFiles = maxDiskFiles;
    }

    private File[] listDiskFiles() {
        File file = new File(this.dataDir);
        return file.listFiles(fileName -> DATA_FILE_RE.matcher(fileName.getName()).matches());
    }

    public synchronized long getMaxDiskId() {
        File[] files = listDiskFiles();
        long maxFileId = -1;
        for (File file : files) {
            Matcher matcher = DATA_FILE_RE.matcher(file.getName());
            if (matcher.matches()) {
                maxFileId = Math.max(Long.parseLong(matcher.group(1)), maxFileId);
            }
        }
        return maxFileId;
    }

    public synchronized long nextDiskFileId() {
        return maxFileId.incrementAndGet();
    }

    public void addDiskFile(DiskFile diskFile) {
        synchronized (diskFiles) {
            diskFiles.add(diskFile);
        }
    }

    public synchronized void addDiskFile(String fileName) throws IOException {
        DiskFile diskFile = new DiskFile();
        diskFile.open(fileName);
        addDiskFile(diskFile);
    }

    public synchronized String getNextDiskFileName() {
        return new File(this.dataDir, String.format("data.%20d", nextDiskFileId())).toString();
    }

    public void open() throws IOException {
        File[] files = listDiskFiles();
        for (File file : files) {
            DiskFile diskFile = new DiskFile();
            diskFile.open(file.getAbsolutePath());
            diskFiles.add(diskFile);
        }
        maxFileId = new AtomicLong(getMaxDiskId());
    }

    public List<DiskFile> getDiskFiles() {
        synchronized (diskFiles) {
            return new ArrayList<>(diskFiles);
        }
    }

    public void removeDiskFiles(Collection<DiskFile> filesToRemove) {
        synchronized (filesToRemove) {
            diskFiles.removeAll(filesToRemove);
        }
    }

    public long getMaxDiskFiles() {
        return this.maxDiskFiles;
    }

    @Override
    public void close() throws IOException {
        IOException closedException = null;
        for (DiskFile diskFile : diskFiles) {
            try {
                diskFile.close();
            } catch (IOException e) {
                closedException = e;
            }
        }
        if (closedException != null) {
            throw closedException;
        }
    }

    public SeekIter<KeyValue> createIterator(List<DiskFile> diskFiles) throws IOException {
        List<SeekIter<KeyValue>> iterList = new ArrayList<>();
        diskFiles.forEach(diskFile -> iterList.add(diskFile.iterator()));
        return new MultiIter(iterList);
    }

    public SeekIter<KeyValue> createIterator() throws IOException {
        return createIterator(getDiskFiles());
    }

    public static class MultiIter implements SeekIter<KeyValue> {

        private class IterNode {
            KeyValue kv;
            SeekIter<KeyValue> iter;

            public IterNode(KeyValue kv, SeekIter<KeyValue> iter) {
                this.kv = kv;
                this.iter = iter;
            }
        }

        private SeekIter<KeyValue>[] iterList;
        private PriorityQueue<IterNode> queue;

        public MultiIter(SeekIter<KeyValue>[] iterList) throws IOException {
            Requires.requireNotNull(iterList);
            this.iterList = iterList;
            this.queue = new PriorityQueue<>();
            for (SeekIter<KeyValue> seekIter : iterList) {
                if (seekIter != null && seekIter.hasNext()) {
                    queue.add(new IterNode(seekIter.next(), seekIter));
                }
            }
        }

        @SuppressWarnings("unchecked")
        public MultiIter(List<SeekIter<KeyValue>> iterList) throws IOException {
            this(iterList.toArray(new SeekIter[0]));
        }

        @Override
        public boolean hasNext() throws IOException {
            return queue.size() > 0;
        }

        @Override
        public KeyValue next() throws IOException {
            while (!queue.isEmpty()) {
                IterNode first = queue.poll();
                if (first.kv != null) {
                    if (first.iter.hasNext()) {
                        queue.add(new IterNode(first.iter.next(), first.iter));
                    }
                }
                return first.kv;
            }
            return null;
        }

        @Override
        public void seekTo(KeyValue target) throws IOException {
            queue.clear();
            for (SeekIter<KeyValue> it : iterList) {
                it.seekTo(target);
                if (it.hasNext()) {
                    queue.add(new IterNode(it.next(), it));
                }
            }
        }
    }



}
