package org.apache.cassandra.db;

import io.reactivex.Completable;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public interface IMutation {
   void apply();

   Completable applyAsync();

   String getKeyspaceName();

   Collection<TableId> getTableIds();

   DecoratedKey key();

   long getTimeout();

   String toString(boolean var1);

   Collection<PartitionUpdate> getPartitionUpdates();

   PartitionUpdate get(TableMetadata var1);

   IMutation add(PartitionUpdate var1);

   static default long dataSize(Collection<? extends IMutation> mutations) {
      long size = 0L;
      Iterator var3 = mutations.iterator();

      while(var3.hasNext()) {
         IMutation mutation = (IMutation)var3.next();

         PartitionUpdate update;
         for(Iterator var5 = mutation.getPartitionUpdates().iterator(); var5.hasNext(); size += (long)update.dataSize()) {
            update = (PartitionUpdate)var5.next();
         }
      }

      return size;
   }
}
