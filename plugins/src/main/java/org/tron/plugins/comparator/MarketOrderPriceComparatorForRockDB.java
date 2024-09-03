package org.tron.plugins.comparator;

import java.nio.ByteBuffer;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.AbstractComparator;
import org.tron.plugins.utils.MarketUtils;

public  class MarketOrderPriceComparatorForRockDB extends AbstractComparator {

  public MarketOrderPriceComparatorForRockDB(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  public String name() {
    return "MarketOrderPriceComparator";
  }

  @Override
  public int compare(final ByteBuffer a, final ByteBuffer b) {
    return MarketUtils.comparePriceKey(a.array(), b.array());
  }

}