package org.tron.leveldb;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class LevelDBTest {

    public static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }

    @Test
    public void testInit() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        try {
            Options options = new Options().createIfMissing(true);
            DB db = new DB(f, options);
            db.close();
        } finally {
            deleteDirectory(f);
        }
    }

    @Test
    public void testPutGet() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        try {
            Options options = new Options().createIfMissing(true);
            try (DB db = new DB(f, options)) {
                db.put("key".getBytes(), "value".getBytes());
                byte[] value = db.get("key".getBytes());
                assertEquals("value", new String(value));
            }
        } finally {
            deleteDirectory(f);
        }
    }

    @Test
    public void testPutDelete() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        try {
            Options options = new Options().createIfMissing(true);
            try (DB db = new DB(f, options)) {
                db.put("key".getBytes(), "value".getBytes());
                db.delete("key".getBytes(), false);
                byte[] value = db.get("key".getBytes());
                assertNull(value);
            }
        } finally {
            deleteDirectory(f);
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testCreateIfMissing() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        new DB(f, new Options().createIfMissing(false));
    }

    @Test
    public void testLogger() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        try {
            ArrayList<String> logMessages = new ArrayList<String>();
            Options options = new Options().createIfMissing(true).logger(message -> logMessages.add(message));
            try (DB db = new DB(f, options)) {
                assertTrue(logMessages.size() > 0);
                assertTrue(logMessages.get(0).matches("Creating DB .* since it was missing."));
            }
        } finally {
            deleteDirectory(f);
        }
    }

    @Test
    public void testIterator() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        try {
            Options options = new Options().createIfMissing(true);
            try (DB db = new DB(f, options)) {
                db.put("key1".getBytes(), "value1".getBytes());
                db.put("key2".getBytes(), "value2".getBytes());
                db.put("key3".getBytes(), "value3".getBytes());
                try (DBIterator iterator = db.iterator(new ReadOptions())) {
                    iterator.seekToFirst();
                    assertTrue(iterator.hasNext());
                    assertEquals("key1", new String(iterator.next().getKey()));
                    assertTrue(iterator.hasNext());
                    assertEquals("key2", new String(iterator.next().getKey()));
                    assertTrue(iterator.hasNext());
                    assertEquals("key3", new String(iterator.next().getKey()));
                    assertFalse(iterator.hasNext());
                }
            }
        } finally {
            deleteDirectory(f);
        }
    }

    @Test
    public void testWriteBatch() throws IOException {
        File f = new File(new File(System.getProperty("java.io.tmpdir")), "testleveldb");
        try {
            Options options = new Options().createIfMissing(true);
            try (DB db = new DB(f, options)) {
                try (WriteBatch wb = db.createWriteBatch()) {
                    wb.put("key0".getBytes(), "value0".getBytes());
                    wb.put("key1".getBytes(), "value1".getBytes());
                    wb.put("key2".getBytes(), "value2".getBytes());
                    wb.put("key3".getBytes(), "value3".getBytes());
                    wb.delete("key0".getBytes());
                    db.write(wb, true);
                }
                try (DBIterator iterator = db.iterator(new ReadOptions())) {
                    iterator.seekToFirst();
                    assertTrue(iterator.hasNext());
                    assertEquals("key1", new String(iterator.next().getKey()));
                    assertTrue(iterator.hasNext());
                    assertEquals("key2", new String(iterator.next().getKey()));
                    assertTrue(iterator.hasNext());
                    assertEquals("key3", new String(iterator.next().getKey()));
                    assertFalse(iterator.hasNext());
                }
            }
        } finally {
            deleteDirectory(f);
        }
    }
}