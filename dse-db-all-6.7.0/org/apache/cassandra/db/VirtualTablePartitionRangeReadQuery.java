package org.apache.cassandra.db;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public class VirtualTablePartitionRangeReadQuery extends VirtualTableReadQuery implements PartitionRangeReadQuery {
   private final DataRange dataRange;

   public static VirtualTablePartitionRangeReadQuery create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange) {
      return new VirtualTablePartitionRangeReadQuery(metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, TPCTaskType.READ_LOCAL);
   }

   private VirtualTablePartitionRangeReadQuery(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange, TPCTaskType readType) {
      super(metadata, nowInSec, columnFilter, rowFilter, limits, readType);
      this.dataRange = dataRange;
   }

   public DataRange dataRange() {
      return this.dataRange;
   }

   public PartitionRangeReadQuery withUpdatedLimit(DataLimits newLimits) {
      return new VirtualTablePartitionRangeReadQuery(this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), newLimits, this.dataRange(), this.readType);
   }

   public PartitionRangeReadQuery withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange) {
      return new VirtualTablePartitionRangeReadQuery(this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), newLimits, newDataRange, this.readType);
   }

   protected Flow<FlowableUnfilteredPartition> querySystemView() {
      VirtualTable table = Schema.instance.getVirtualTableInstance(this.metadata().id);
      return table.select(this.dataRange, this.columnFilter());
   }

   protected void appendCQLWhereClause(StringBuilder sb) {
      if(!this.dataRange.isUnrestricted() || !this.rowFilter().isEmpty()) {
         sb.append(" WHERE ");
         if(!this.rowFilter().isEmpty()) {
            sb.append(this.rowFilter());
            if(!this.dataRange.isUnrestricted()) {
               sb.append(" AND ");
            }
         }

         if(!this.dataRange.isUnrestricted()) {
            sb.append(this.dataRange.toCQLString(this.metadata()));
         }

      }
   }
}
