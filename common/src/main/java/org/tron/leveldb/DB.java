package org.tron.leveldb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

public class DB implements AutoCloseable {
    private boolean verifyChecksumsSet;
    private native void init(File file, Options dbOptions) throws IOException;

    public DB(File file, Options dbOptions) throws IOException {
        init(file, dbOptions);
        if (dbOptions.verifyChecksums()) {
            verifyChecksumsSet = true;
        }
    }

    public static void pushMemoryPool(int i) {
    }

    public static void popMemoryPool() {
    }

    public native DBIterator iterator(ReadOptions fillCache);

    public native void close() throws IOException;

    public native void put(byte[] key, byte[] data);

    public native void put(byte[] key, byte[] data, boolean sync);

    public native byte[] get(byte[] key);

    public native void delete(byte[] key, boolean sync);

    public native WriteBatch createWriteBatch();

    public native void write(WriteBatch batch, boolean sync) throws IOException;

    public native String getProperty(String s);

    private long nativeDb;
    private long nativeComparator;
    private long nativeLogger;

    static {
        if (System.getenv("LEVELDB_PATH") != null) {
            System.load(System.getenv("LEVELDB_PATH"));
        } else {
            final String arch = System.getProperty("os.arch").toLowerCase(Locale.ENGLISH);
            final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
            String library = null;
			String ext = null;
			if ((arch.contains("86") || arch.contains("amd")) && arch.contains("64"))
				if (os.contains("win")) {
					library = "leveldb-jni.dll";
					ext = ".dll";
				}
				else if (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0) {
					library = "libleveldb-jni.so";
					ext = ".so";
				}
            if (library == null) System.err.println("Cannot find leveldb library for arch=" + arch + " os=" + os);
            else {
                InputStream is = DB.class.getResourceAsStream(library);
                if (is == null) {
                    System.err.println("Cannot load the leveldb library: " + library);
                } else {
                    try {
                        File temp = File.createTempFile("leveldb", ext);
                        temp.deleteOnExit();
                        try {
                            FileOutputStream fos = new FileOutputStream(temp);
                            byte[] buffer = new byte[1024];
                            int read;
                            while ((read = is.read(buffer)) != -1) {
                                fos.write(buffer, 0, read);
                            }
                            fos.close();
                            System.load(temp.getAbsolutePath());
                        } finally {
                            temp.delete();
                        }
                    } catch (IOException e) {
                        System.err.println("Cannot load the leveldb library: " + library + " due to " + e);
                    }
                }
            }
        }
    }
}
