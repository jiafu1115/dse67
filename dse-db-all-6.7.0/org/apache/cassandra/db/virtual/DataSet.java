package org.apache.cassandra.db.virtual;

import io.reactivex.Completable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public interface DataSet {
   Completable addRow(Object var1);

   Completable addRow(Object var1, DataSet.RowBuilder var2);

   DataSet.RowBuilder newRowBuilder(Object... var1);

   boolean isEmpty();

   Completable apply(PartitionUpdate var1);

   Flow<DataSet.Partition> getPartition(DecoratedKey var1);

   Flow<DataSet.Partition> getPartitions(DataRange var1);

   int getAverageColumnSize();

   public interface RowBuilder {
      DataSet.RowBuilder addColumn(String var1, Supplier<?> var2);

      DataSet.RowBuilder addColumn(String var1, Supplier<?> var2, Consumer<?> var3);
   }

   public interface Partition {
      DecoratedKey key();

      FlowableUnfilteredPartition toFlowable(TableMetadata var1, ClusteringIndexFilter var2, ColumnFilter var3, long var4);
   }
}
