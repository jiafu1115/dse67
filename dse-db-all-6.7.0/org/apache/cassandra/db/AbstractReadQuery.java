package org.apache.cassandra.db;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public abstract class AbstractReadQuery implements ReadQuery {
   private final TableMetadata metadata;
   private final int nowInSec;
   private final ColumnFilter columnFilter;
   private final RowFilter rowFilter;
   private final DataLimits limits;
   protected final TPCTaskType readType;

   protected AbstractReadQuery(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, TPCTaskType readType) {
      this.metadata = metadata;
      this.nowInSec = nowInSec;
      this.columnFilter = columnFilter;
      this.rowFilter = rowFilter;
      this.limits = limits;
      this.readType = readType;
   }

   public final TableMetadata metadata() {
      return this.metadata;
   }

   public final DataLimits limits() {
      return this.limits;
   }

   public final int nowInSec() {
      return this.nowInSec;
   }

   public final ColumnFilter columnFilter() {
      return this.columnFilter;
   }

   public final RowFilter rowFilter() {
      return this.rowFilter;
   }

   public final Flow<FlowablePartition> executeInternal(Monitor monitor) {
      return FlowablePartitions.filterAndSkipEmpty(this.executeLocally(monitor), this.nowInSec());
   }

   protected boolean isSame(Object other) {
      if(other != null && this.getClass().equals(other.getClass())) {
         AbstractReadQuery that = (AbstractReadQuery)other;
         return this.metadata.id.equals(that.metadata.id) && this.nowInSec == that.nowInSec && this.columnFilter.equals(that.columnFilter) && this.rowFilter.equals(that.rowFilter) && this.limits.equals(that.limits);
      } else {
         return false;
      }
   }

   public final String toCQLString() {
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT ").append(this.columnFilter());
      sb.append(" FROM ").append(this.metadata().keyspace).append('.').append(this.metadata().name);
      this.appendCQLWhereClause(sb);
      if(this.limits() != DataLimits.NONE) {
         sb.append(' ').append(this.limits());
      }

      return sb.toString();
   }

   protected abstract void appendCQLWhereClause(StringBuilder var1);
}
