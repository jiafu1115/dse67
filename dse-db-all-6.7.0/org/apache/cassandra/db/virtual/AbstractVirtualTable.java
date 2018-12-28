package org.apache.cassandra.db.virtual;

import io.reactivex.Completable;
import io.reactivex.functions.Function;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public abstract class AbstractVirtualTable implements VirtualTable {
   private final TableMetadata metadata;

   protected AbstractVirtualTable(TableMetadata metadata) {
      if(!metadata.isVirtual()) {
         throw new IllegalArgumentException();
      } else {
         this.metadata = metadata;
      }
   }

   public TableMetadata metadata() {
      return this.metadata;
   }

   protected final DataSet newDataSet() {
      return new AbstractDataSet(this.metadata) {
         protected <K, V> NavigableMap<K, V> newNavigableMap(Comparator<? super K> comparator) {
            return new TreeMap(comparator);
         }
      };
   }

   public abstract DataSet data();

   public final Flow<FlowableUnfilteredPartition> select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter) {
      return this.data().getPartition(partitionKey).map((p) -> {
         return p.toFlowable(this.metadata, clusteringIndexFilter, columnFilter, System.currentTimeMillis());
      });
   }

   public final Flow<FlowableUnfilteredPartition> select(DataRange dataRange, ColumnFilter columnFilter) {
      return this.data().getPartitions(dataRange).map((p) -> {
         return p.toFlowable(this.metadata, dataRange.clusteringIndexFilter(p.key()), columnFilter, System.currentTimeMillis());
      });
   }

   public Completable apply(PartitionUpdate update) {
      throw new InvalidRequestException("Modification is not supported by table " + this.metadata);
   }

   public int getAverageColumnSize() {
      return this.data().getAverageColumnSize();
   }
}
