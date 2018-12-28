package org.apache.cassandra.db;

import io.reactivex.functions.Function;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public class VirtualTableSinglePartitionReadQuery extends VirtualTableReadQuery implements SinglePartitionReadQuery {
   private final DecoratedKey partitionKey;
   private final ClusteringIndexFilter clusteringIndexFilter;

   public static VirtualTableSinglePartitionReadQuery create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter) {
      return new VirtualTableSinglePartitionReadQuery(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, TPCTaskType.READ_LOCAL);
   }

   private VirtualTableSinglePartitionReadQuery(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, TPCTaskType readType) {
      super(metadata, nowInSec, columnFilter, rowFilter, limits, readType);
      this.partitionKey = partitionKey;
      this.clusteringIndexFilter = clusteringIndexFilter;
   }

   protected void appendCQLWhereClause(StringBuilder sb) {
      sb.append(" WHERE ");
      sb.append(ColumnMetadata.toCQLString((Iterable)this.metadata().partitionKeyColumns())).append(" = ");
      DataRange.appendKeyString(sb, this.metadata().partitionKeyType, this.partitionKey().getKey());
      if(!this.rowFilter().isEmpty()) {
         sb.append(" AND ").append(this.rowFilter());
      }

      String filterString = this.clusteringIndexFilter.toCQLString(this.metadata());
      if(!filterString.isEmpty()) {
         sb.append(" AND ").append(filterString);
      }

   }

   public ClusteringIndexFilter clusteringIndexFilter() {
      return this.clusteringIndexFilter;
   }

   public boolean selectsFullPartition() {
      return this.clusteringIndexFilter.selectsAllPartition() && !this.rowFilter().hasExpressionOnClusteringOrRegularColumns();
   }

   public DecoratedKey partitionKey() {
      return this.partitionKey;
   }

   public SinglePartitionReadQuery withUpdatedLimit(DataLimits newLimits) {
      return new VirtualTableSinglePartitionReadQuery(this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), newLimits, this.partitionKey(), this.clusteringIndexFilter, TPCTaskType.READ_LOCAL);
   }

   public SinglePartitionReadQuery forPaging(Clustering lastReturned, DataLimits limits, boolean inclusive) {
      return new VirtualTableSinglePartitionReadQuery(this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), limits, this.partitionKey(), lastReturned == null?this.clusteringIndexFilter:this.clusteringIndexFilter.forPaging(this.metadata().comparator, lastReturned, inclusive), TPCTaskType.READ_LOCAL);
   }

   protected Flow<FlowableUnfilteredPartition> querySystemView() {
      VirtualTable table = Schema.instance.getVirtualTableInstance(this.metadata().id);
      return table.select(this.partitionKey, this.clusteringIndexFilter, this.columnFilter());
   }

   public static class Group extends SinglePartitionReadQuery.Group<VirtualTableSinglePartitionReadQuery> {
      public static VirtualTableSinglePartitionReadQuery.Group create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, List<DecoratedKey> partitionKeys, ClusteringIndexFilter clusteringIndexFilter) {
         List<VirtualTableSinglePartitionReadQuery> queries = new ArrayList(partitionKeys.size());
         Iterator var8 = partitionKeys.iterator();

         while(var8.hasNext()) {
            DecoratedKey partitionKey = (DecoratedKey)var8.next();
            queries.add(VirtualTableSinglePartitionReadQuery.create(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter));
         }

         return new VirtualTableSinglePartitionReadQuery.Group(queries, limits);
      }

      public Group(List<VirtualTableSinglePartitionReadQuery> queries, DataLimits limits) {
         super(queries, limits);
      }

      public static VirtualTableSinglePartitionReadQuery.Group one(VirtualTableSinglePartitionReadQuery query) {
         return new VirtualTableSinglePartitionReadQuery.Group(Collections.singletonList(query), query.limits());
      }

      public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException {
         return this.queries.size() == 1?((VirtualTableSinglePartitionReadQuery)this.queries.get(0)).execute(ctx):Flow.fromIterable(this.queries).flatMap((query) -> {
            return query.execute(ctx);
         });
      }
   }
}
