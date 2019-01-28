package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;

public interface PartitionRangeReadQuery extends ReadQuery {
   DataRange dataRange();

   PartitionRangeReadQuery withUpdatedLimit(DataLimits var1);

   PartitionRangeReadQuery withUpdatedLimitsAndDataRange(DataLimits var1, DataRange var2);

   static ReadQuery create(TableMetadata table, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange) {
      return (ReadQuery)(table.isVirtual()?VirtualTablePartitionRangeReadQuery.create(table, nowInSec, columnFilter, rowFilter, limits, dataRange):PartitionRangeReadCommand.create(table, nowInSec, columnFilter, rowFilter, limits, dataRange));
   }

   default QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion) {
      return new PartitionRangeQueryPager(this, pagingState, protocolVersion);
   }

   default boolean selectsKey(DecoratedKey key) {
      return !this.dataRange().contains(key)?false:this.rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, this.metadata().partitionKeyType);
   }

   default boolean selectsClustering(DecoratedKey key, Clustering clustering) {
      return clustering == Clustering.STATIC_CLUSTERING?!this.columnFilter().fetchedColumns().statics.isEmpty():(!this.dataRange().clusteringIndexFilter(key).selects(clustering)?false:this.rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
   }

   default boolean selectsFullPartition() {
      return this.metadata().isStaticCompactTable() || this.dataRange().selectsAllPartition() && !this.rowFilter().hasExpressionOnClusteringOrRegularColumns();
   }
}
