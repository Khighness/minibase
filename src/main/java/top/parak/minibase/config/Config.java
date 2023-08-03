package top.parak.minibase.config;

/**
 * Base config.
 *
 * @author KHighness
 * @since 2023-07-29
 */
public class Config {

    private static final Config DEFAULT = new Config();

    private long   maxMemStoreSize = 16 * 1024 * 1024;
    private int    maxFlushRetries = 10;
    private String dataDir = "MiniBase";
    private int    maxDiskFiles = 10;
    private int    maxThreadPoolSize = 5;

    public long getMaxMemStoreSize() {
        return maxMemStoreSize;
    }

    public Config setMaxMemStoreSize(long maxMemStoreSize) {
        this.maxMemStoreSize = maxMemStoreSize;
        return this;
    }

    public int getMaxFlushRetries() {
        return maxFlushRetries;
    }

    public Config setMaxFlushRetries(int maxFlushRetries) {
        this.maxFlushRetries = maxFlushRetries;
        return this;
    }

    public String getDataDir() {
        return dataDir;
    }

    public Config setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public int getMaxDiskFiles() {
        return maxDiskFiles;
    }

    public Config setMaxDiskFiles(int maxDiskFiles) {
        this.maxDiskFiles = maxDiskFiles;
        return this;
    }

    public int getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    public Config setMaxThreadPoolSize(int maxThreadPoolSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
        return this;
    }

    public static Config getDefault() {
        return DEFAULT;
    }

}
