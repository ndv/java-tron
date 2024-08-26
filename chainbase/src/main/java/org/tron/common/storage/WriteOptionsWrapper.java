package org.tron.common.storage;

public class WriteOptionsWrapper {

  public org.rocksdb.WriteOptions rocks = null;
  public boolean sync;

  private WriteOptionsWrapper() {

  }

  public static WriteOptionsWrapper getInstance() {
    WriteOptionsWrapper wrapper = new WriteOptionsWrapper();
    wrapper.rocks = new org.rocksdb.WriteOptions();

    return wrapper;
  }


  public WriteOptionsWrapper sync(boolean bool) {
    this.sync = bool;
    this.rocks.setSync(bool);
    return this;
  }
}
