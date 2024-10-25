package org.tron.common.utils;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.AbstractComparator;
import org.tron.core.capsule.utils.MarketUtils;

import java.nio.ByteBuffer;

public class MarketOrderPriceComparatorForRockDB extends AbstractComparator {

  public MarketOrderPriceComparatorForRockDB(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  public String name() {
    return "MarketOrderPriceComparator";
  }

  @Override
  public int compare(final ByteBuffer a, final ByteBuffer b) {
    return MarketUtils.comparePriceKey(convertDataToBytes(a), convertDataToBytes(b));
  }

  /**
   * DirectSlice.data().array will throw UnsupportedOperationException.
   * */
  public byte[] convertDataToBytes(ByteBuffer directSlice) {
    int capacity = directSlice.capacity();
    byte[] bytes = new byte[capacity];

    for (int i = 0; i < capacity; i++) {
      bytes[i] = directSlice.get(i);
    }

    return bytes;
  }

}
