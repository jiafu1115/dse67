package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

public class SinglePartitionPager extends AbstractQueryPager<SinglePartitionReadQuery> {
   private volatile PagingState.RowMark lastReturned;

   public SinglePartitionPager(SinglePartitionReadQuery query, PagingState state, ProtocolVersion protocolVersion) {
      super(query, protocolVersion);
      if(state != null) {
         this.lastReturned = state.rowMark;
         this.restoreState(query.partitionKey(), state.remaining, state.remainingInPartition, state.inclusive);
      }

   }

   private SinglePartitionPager(SinglePartitionReadQuery query, ProtocolVersion protocolVersion, PagingState.RowMark rowMark, int remaining, int remainingInPartition) {
      super(query, protocolVersion);
      this.lastReturned = rowMark;
      this.restoreState(query.partitionKey(), remaining, remainingInPartition, false);
   }

   public SinglePartitionPager withUpdatedLimit(DataLimits newLimits) {
      return new SinglePartitionPager(((SinglePartitionReadQuery)this.query).withUpdatedLimit(newLimits), this.protocolVersion, this.lastReturned, this.maxRemaining(), this.remainingInPartition());
   }

   public ByteBuffer key() {
      return ((SinglePartitionReadQuery)this.query).partitionKey().getKey();
   }

   protected PagingState makePagingState(DecoratedKey lastKey, Row lastRow, boolean inclusive) {
      return this.makePagingState(this.getLastReturned(lastRow), inclusive);
   }

   protected PagingState makePagingState(boolean inclusive) {
      return this.makePagingState(this.lastReturned, inclusive);
   }

   private PagingState makePagingState(PagingState.RowMark lastRow, boolean inclusive) {
      int maxRemaining = inclusive?FBUtilities.add(this.maxRemaining(), 1):this.maxRemaining();
      int remainingInPartition = inclusive?FBUtilities.add(this.remainingInPartition(), 1):this.remainingInPartition();
      return lastRow == null?null:new PagingState((ByteBuffer)null, lastRow, maxRemaining, remainingInPartition, inclusive);
   }

   protected ReadQuery nextPageReadQuery(DataLimits limits, PageSize pageSize) {
      Clustering clustering = this.lastReturned == null?null:this.lastReturned.clustering(((SinglePartitionReadQuery)this.query).metadata());
      limits = this.lastReturned == null?limits.forPaging(pageSize):limits.forPaging(pageSize, this.key(), this.remainingInPartition());
      return ((SinglePartitionReadQuery)this.query).forPaging(clustering, limits, this.inclusive);
   }

   protected void recordLast(DecoratedKey key, Row last) {
      this.lastReturned = this.getLastReturned(last);
   }

   private PagingState.RowMark getLastReturned(Row last) {
      return last != null && last.clustering() != Clustering.STATIC_CLUSTERING?PagingState.RowMark.create(((SinglePartitionReadQuery)this.query).metadata(), last, this.protocolVersion):this.lastReturned;
   }

   protected boolean isPreviouslyReturnedPartition(DecoratedKey key) {
      return this.lastReturned != null;
   }
}
