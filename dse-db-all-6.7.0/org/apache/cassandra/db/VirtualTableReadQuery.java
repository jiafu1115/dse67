package org.apache.cassandra.db;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public abstract class VirtualTableReadQuery extends AbstractReadQuery {
   protected VirtualTableReadQuery(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, TPCTaskType readType) {
      super(metadata, nowInSec, columnFilter, rowFilter, limits, readType);
   }

   public ReadExecutionController executionController() {
      throw new UnsupportedOperationException();
   }

   protected abstract Flow<FlowableUnfilteredPartition> querySystemView();

   public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException {
      return this.executeInternal();
   }

   public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor) {
      Flow<FlowableUnfilteredPartition> r = this.querySystemView();
      r = this.rowFilter().filter(r, this.metadata(), this.nowInSec());
      return this.limits().truncateUnfiltered(r, this.nowInSec(), this.selectsFullPartition(), this.metadata().rowPurger());
   }

   public boolean queriesOnlyLocalData() {
      return true;
   }
}
