package top.parak.minibase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.parak.minibase.Iter;
import top.parak.minibase.KeyValue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Disk store compactor.
 *
 * @author Khighness
 * @since 2023-08-06
 */
public class DiskStoreCompactor extends Compactor {

    private static final Logger LOG = LoggerFactory.getLogger(DiskStoreCompactor.class);

    private DiskStore        diskStore;
    private volatile boolean running = true;

    public DiskStoreCompactor(DiskStore diskStore) {
        this.diskStore = diskStore;
        this.setDaemon(true);
    }

    private void performCompact(List<DiskFile> filesToCompact) throws IOException {
        if (filesToCompact == null || filesToCompact.isEmpty()) {
            return;
        }

        String fileName = diskStore.getNextDiskFileName();
        String fileTempName = fileName + DiskStore.FILE_NAME_TMP_SUFFIX;
        try {
            try (DiskFileWriter writer = new DiskFileWriter(fileTempName)) {
                for (Iter<KeyValue> iter = diskStore.createIterator(filesToCompact); iter.hasNext();) {
                    writer.append(iter.next());
                }
                writer.appendIndex();
                writer.appendTrailer();
            }

            File file = new File(fileTempName);
            if (!file.renameTo(new File(fileName))) {
                throw new IOException("Rename " + fileTempName + " to " + fileName + " failed");
            }

            for (DiskFile diskFile : filesToCompact) {
                diskFile.close();
                File oldFile = new File(diskFile.getFileName());
                File archliveFile = new File(diskFile.getFileName() + DiskStore.FILE_NAME_ARCHIVE_SUFFIX);
                if (!oldFile.renameTo(archliveFile)) {
                    LOG.error("Perform compact, failed to rename file {} to archive file {}",
                            oldFile.getName(), archliveFile.getName());
                }
            }
            diskStore.removeDiskFiles(filesToCompact);
            diskStore.addDiskFile(fileName);
        } finally {
            File file = new File(fileTempName);
            if (file.exists()) {
                file.delete();
            }
        }

        List<String> diskFileNames = filesToCompact.stream().map(DiskFile::getFileName).collect(Collectors.toList());
        LOG.info("Perform compact, {} -> {}", diskFileNames, fileName);
    }

    @Override
    public void compact() throws IOException {
        List<DiskFile> filesToCompact = new ArrayList<>(diskStore.getDiskFiles());
        performCompact(filesToCompact);
    }

    @Override
    public void run() {
        while (running) {
            try {
                boolean isCompacted = false;
                if (diskStore.getDiskFiles().size() > diskStore.getMaxDiskFiles()) {
                    performCompact(diskStore.getDiskFiles());
                    isCompacted = true;
                }
                if (!isCompacted) {
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                LOG.error("Major compaction failed", e);
            } catch (InterruptedException ie) {
                LOG.error("Major compaction interrupted, stop running", ie);
                break;
            }
        }
    }

}
