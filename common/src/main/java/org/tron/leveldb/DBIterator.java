package org.tron.leveldb;

import java.io.IOException;
import java.util.Map;

public class DBIterator implements AutoCloseable {

    public DBIterator(long nativeIterator) {
        this.nativeHandle = nativeIterator;
    }

    @Override
    public native void close() throws IOException;

    public native void seekToFirst();

    public native boolean hasNext();

    public native Map.Entry<byte[],byte[]> next();

    public native Map.Entry<byte[],byte[]> peekNext();

    public native void seekToLast();

    public native boolean hasPrev();

    public native void prev();

    public native Map.Entry<byte[],byte[]> peekPrev();

    public native void seek(byte[] key);

    private long nativeHandle;
}
