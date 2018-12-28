package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.RowPurger;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

final class Filter extends Transformation {
   private final int nowInSec;
   private final RowPurger rowPurger;

   public Filter(int nowInSec, RowPurger rowPurger) {
      this.nowInSec = nowInSec;
      this.rowPurger = rowPurger;
   }

   protected RowIterator applyToPartition(BaseRowIterator iterator) {
      return iterator instanceof UnfilteredRows?new FilteredRows(this, (UnfilteredRows)iterator):new FilteredRows((UnfilteredRowIterator)iterator, this);
   }

   public Row applyToStatic(Row row) {
      if(row.isEmpty()) {
         return Rows.EMPTY_STATIC_ROW;
      } else {
         row = row.purge(DeletionPurger.PURGE_ALL, this.nowInSec, this.rowPurger);
         return row == null?Rows.EMPTY_STATIC_ROW:row;
      }
   }

   protected Row applyToRow(Row row) {
      return row.purge(DeletionPurger.PURGE_ALL, this.nowInSec, this.rowPurger);
   }

   protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
      return null;
   }
}
