package org.tron.leveldb;

public class ReadOptions {
    private boolean fillCache;
    private boolean verifyChecksums;

    public ReadOptions fillCache(boolean b) {
        this.fillCache = b;
        return this;
    }

    public boolean fillCache() {
        return fillCache;
    }

    public ReadOptions verifyChecksums(boolean b) {
        this.verifyChecksums = b;
        return this;
    }

    public boolean verifyChecksums() {
        return verifyChecksums;
    }

}
