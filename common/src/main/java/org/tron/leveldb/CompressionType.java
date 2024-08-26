package org.tron.leveldb;

public enum CompressionType {
    NONE(0),
    SNAPPY(1);

    public static CompressionType getCompressionTypeByPersistentId(int parseInt) {
        if (parseInt == 0) return NONE;
        else return SNAPPY;
    }

    private final int persistentId;

    CompressionType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int persistentId()
    {
        return persistentId;
    }
}
