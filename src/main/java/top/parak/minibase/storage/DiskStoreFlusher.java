package top.parak.minibase.storage;

import top.parak.minibase.Iter;
import top.parak.minibase.KeyValue;

import java.io.File;
import java.io.IOException;

/**
 * Disk store flusher.
 *
 * @author cantai
 * @since 2023-08-08
 */
public class DiskStoreFlusher implements Flusher {

    private DiskStore diskStore;

    public DiskStoreFlusher(DiskStore diskStore) {
        this.diskStore = diskStore;
    }

    @Override
    public void flush(Iter<KeyValue> it) throws IOException {
        String fileName = diskStore.getNextDiskFileName();
        String fileTempName = fileName + DiskStore.FILE_NAME_ARCHIVE_SUFFIX;
        try {
            try (DiskFileWriter writer = new DiskFileWriter(fileTempName)) {
                while (it.hasNext()) {
                    writer.append(it.next());
                }
                writer.appendIndex();
                writer.appendTrailer();
            }

            File file = new File(fileTempName);
            if (!file.renameTo(new File(fileName))) {
                throw new IOException("Rename " + fileTempName + " to " + fileName + " failed");
            }
            diskStore.addDiskFile(fileName);
        } finally {
            File file = new File(fileTempName);
            if (file.exists()) {
                file.delete();
            }
        }
    }
}
