package org.tron.leveldb;

public interface DBComparator {
    String name();

    byte[] findShortestSeparator(byte[] start, byte[] limit);

    byte[] findShortSuccessor(byte[] key);

    int compare(byte[] o1, byte[] o2);
}
