package top.parak.minibase.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Disk store.
 *
 * @author Khighness
 * @since 2023-08-05
 */
public class DiskStore implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DiskStore.class);
    private static final String FILE_NAME_TMP_SUFFIX = ".tmp";
    private static final String FILE_NAME_ARCHIVE_SUFFIX = ".archive";
    private static final Pattern DATA_FILE_RE = Pattern.compile("data\\.([0-9]+)");

    private String dataDir;
    private List<DiskFile> diskFiles;

    private int maxDiskFiles;
    private volatile AtomicLong maxField;

    public DiskFile(String dataDir, int maxDiskFiles) {
        this.dataDir = dataDir;
        this.diskFiles = new ArrayList<>();
        this.maxDiskFiles = maxDiskFiles;
    }

    @Override
    public void close() throws IOException {

    }

}
