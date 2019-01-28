package org.apache.cassandra.db;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.RowPurger;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.MultiPartitionPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.flow.Flow;

public interface SinglePartitionReadQuery extends ReadQuery {
   DecoratedKey partitionKey();

   SinglePartitionReadQuery withUpdatedLimit(DataLimits var1);

   SinglePartitionReadQuery forPaging(Clustering var1, DataLimits var2, boolean var3);

   ClusteringIndexFilter clusteringIndexFilter();

   default SinglePartitionPager getPager(PagingState pagingState, ProtocolVersion protocolVersion) {
      return new SinglePartitionPager(this, pagingState, protocolVersion);
   }

   default boolean selectsClustering(DecoratedKey key, Clustering clustering) {
      return clustering == Clustering.STATIC_CLUSTERING?!this.columnFilter().fetchedColumns().statics.isEmpty():(!this.clusteringIndexFilter().selects(clustering)?false:this.rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
   }

   default boolean selectsKey(DecoratedKey key) {
      return !this.partitionKey().equals(key)?false:this.rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, this.metadata().partitionKeyType);
   }

   static SinglePartitionReadQuery.Group<? extends SinglePartitionReadQuery> createGroup(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, List<DecoratedKey> partitionKeys, ClusteringIndexFilter clusteringIndexFilter) {
      return (SinglePartitionReadQuery.Group)(metadata.isVirtual()?VirtualTableSinglePartitionReadQuery.Group.create(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKeys, clusteringIndexFilter):SinglePartitionReadCommand.Group.create(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKeys, clusteringIndexFilter));
   }

   public abstract static class Group<T extends SinglePartitionReadQuery> implements ReadQuery {
      public final List<T> queries;
      private final DataLimits limits;
      private final int nowInSec;
      private final boolean selectsFullPartitions;
      private final RowPurger rowPurger;

      protected Group(List<T> queries, DataLimits limits) {
         assert !queries.isEmpty();

         this.queries = queries;
         this.limits = limits;
         T firstQuery = (T)queries.get(0);
         this.nowInSec = firstQuery.nowInSec();
         this.selectsFullPartitions = firstQuery.selectsFullPartition();
         this.rowPurger = firstQuery.metadata().rowPurger();

         for(int i = 1; i < queries.size(); ++i) {
            assert ((SinglePartitionReadQuery)queries.get(i)).nowInSec() == this.nowInSec;
         }

      }

      public int nowInSec() {
         return this.nowInSec;
      }

      public DataLimits limits() {
         return this.limits;
      }

      public TableMetadata metadata() {
         return ((SinglePartitionReadQuery)this.queries.get(0)).metadata();
      }

      public boolean isEmpty() {
         return false;
      }

      public ReadContext.Builder applyDefaults(ReadContext.Builder ctx) {
         return ((SinglePartitionReadQuery)this.queries.get(0)).applyDefaults(ctx);
      }

      public boolean selectsFullPartition() {
         return this.selectsFullPartitions;
      }

      public ReadExecutionController executionController() {
         return ((SinglePartitionReadQuery)this.queries.get(0)).executionController();
      }

      public Flow<FlowablePartition> executeInternal(Monitor monitor) {
         return this.limits.truncateFiltered(FlowablePartitions.filterAndSkipEmpty(this.executeLocally(monitor, false), this.nowInSec()), this.nowInSec(), this.selectsFullPartitions, this.rowPurger);
      }

      public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor) {
         return this.executeLocally(monitor, true);
      }

      private Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor, boolean sort) {
         if(this.queries.size() == 1) {
            return ((SinglePartitionReadQuery)this.queries.get(0)).executeLocally(monitor);
         } else {
            List<T> queries = this.queries;
            if(sort) {
               queries = new ArrayList((Collection)queries);
               ((List)queries).sort(Comparator.comparing(SinglePartitionReadQuery::partitionKey));
            }

            return Flow.fromIterable(queries).flatMap((query) -> {
               return query.executeLocally(monitor);
            });
         }
      }

      public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion) {
         return (QueryPager)(this.queries.size() == 1?((SinglePartitionReadQuery)this.queries.get(0)).getPager(pagingState, protocolVersion):new MultiPartitionPager(this, pagingState, protocolVersion));
      }

      public boolean selectsKey(DecoratedKey key) {
         return Iterables.any(this.queries, (c) -> {
            return c.selectsKey(key);
         });
      }

      public boolean selectsClustering(DecoratedKey key, Clustering clustering) {
         return Iterables.any(this.queries, (c) -> {
            return c.selectsClustering(key, clustering);
         });
      }

      public boolean queriesOnlyLocalData() {
         Iterator var1 = this.queries.iterator();

         SinglePartitionReadQuery query;
         do {
            if(!var1.hasNext()) {
               return true;
            }

            query = (SinglePartitionReadQuery)var1.next();
         } while(query.queriesOnlyLocalData());

         return false;
      }

      public RowFilter rowFilter() {
         return ((SinglePartitionReadQuery)this.queries.get(0)).rowFilter();
      }

      public ColumnFilter columnFilter() {
         return ((SinglePartitionReadQuery)this.queries.get(0)).columnFilter();
      }

      public String toCQLString() {
         return Joiner.on("; ").join(Iterables.transform(this.queries, ReadQuery::toCQLString));
      }

      public String toString() {
         return this.queries.toString();
      }
   }
}
