package org.tron.leveldb;

import java.io.IOException;

public class WriteBatch implements AutoCloseable {
    public WriteBatch(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    @Override
    public native void close() throws IOException;

    public native void delete(byte[] key);

    public native void put(byte[] key, byte[] value);

    public long nativeHandle;
}
