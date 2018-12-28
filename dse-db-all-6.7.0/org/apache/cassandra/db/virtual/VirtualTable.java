package org.apache.cassandra.db.virtual;

import io.reactivex.Completable;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public interface VirtualTable {
   default String name() {
      return this.metadata().name;
   }

   TableMetadata metadata();

   int getAverageColumnSize();

   Completable apply(PartitionUpdate var1);

   Flow<FlowableUnfilteredPartition> select(DecoratedKey var1, ClusteringIndexFilter var2, ColumnFilter var3);

   Flow<FlowableUnfilteredPartition> select(DataRange var1, ColumnFilter var2);
}
