package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;

public interface SSTableFlushObserver {
   void begin();

   void startPartition(DecoratedKey var1, long var2);

   void partitionLevelDeletion(DeletionTime var1, long var2);

   void staticRow(Row var1, long var2);

   default void nextUnfilteredCluster(Unfiltered unfiltered) {
   }

   default void nextUnfilteredCluster(Unfiltered unfiltered, long position) {
      this.nextUnfilteredCluster(unfiltered);
   }

   void complete();
}
