package org.tron.leveldb;

public class Options {
    private boolean createIfMissing = true;
    private boolean paranoidChecks;
    private boolean verifyChecksums = true;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private int blockSize = 4 * 1024;
    private int writeBufferSize = 4 * 1024 * 1024;
    private long cacheSize;
    private int maxOpenFiles = 1000;
    private DBComparator comparator;
    private Logger logger;

    public Options createIfMissing(boolean v) {
        this.createIfMissing = v;
        return this;
    }

    public Options paranoidChecks(boolean b) {
        this.paranoidChecks = b;
        return this;
    }

    public Options verifyChecksums(boolean b) {
        this.verifyChecksums = b;
        return this;
    }

    public Options compressionType(CompressionType t) {
        this.compressionType = t;
        return this;
    }

    public Options blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public Options writeBufferSize(int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public Options cacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public Options maxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public boolean createIfMissing() { return createIfMissing; }

    public boolean paranoidChecks() { return paranoidChecks; }

    public boolean verifyChecksums() { return verifyChecksums; }

    public CompressionType compressionType() { return compressionType; }

    public int blockSize() { return blockSize; }

    public int writeBufferSize() { return writeBufferSize; }

    public long cacheSize() { return cacheSize; }

    public int maxOpenFiles() { return maxOpenFiles; }

    public Options comparator(DBComparator comparator) {
        this.comparator = comparator;
        return this;
    }

    public Options logger(Logger leveldbLogger) {
        this.logger = leveldbLogger;
        return this;
    }
}
