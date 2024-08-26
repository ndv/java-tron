package org.tron.core.vm.repository;

import lombok.Getter;

public class WriteOptionsWrapper {

  @Getter
  private org.rocksdb.WriteOptions rocks = null;
  @Getter
  private boolean sync;

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
