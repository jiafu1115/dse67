package org.apache.cassandra.db.virtual;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.cassandra.schema.TableMetadata;

public abstract class InMemoryVirtualTable<T> extends AbstractVirtualTable {
   private final DataSet dataSet;

   protected InMemoryVirtualTable(TableMetadata metadata) {
      super(metadata);
      this.dataSet = new AbstractDataSet(metadata) {
         protected <K, V> NavigableMap<K, V> newNavigableMap(Comparator<? super K> comparator) {
            return new ConcurrentSkipListMap(comparator);
         }
      };
   }

   public final DataSet data() {
      return this.dataSet;
   }

   public abstract void addRowData(T var1);
}
