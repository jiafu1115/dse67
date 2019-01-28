package org.apache.cassandra.db.rows;

import java.util.function.BiFunction;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.btree.BTree;

public interface RowPurger {
   RowPurger PURGE_EMPTY_ROWS = new RowPurger.PurgeEmptyRows();
   RowPurger PURGE_ROWS_WITH_EMPTY_PRIMARY_KEY = new RowPurger.PurgeRowsWithEmptyPrimaryKey();

   static RowPurger purgeRowsWithoutRequiredColumns(int requiredColumnsForLiveness) {
      return new RowPurger.PurgeRowsWithoutRequiredColumns(requiredColumnsForLiveness);
   }

   boolean shouldPurgeRow(LivenessInfo var1, Row.Deletion var2, ColumnData[] var3, int var4);

   boolean hasLiveData(LivenessInfo var1, int var2, BiFunction<Boolean, BTree.ReduceFunction<Boolean, Cell>, Boolean> var3);

   Pair<Boolean, Integer> isAliveRowAndCountTombstones(Row var1, int var2);

   public static class PurgeRowsWithoutRequiredColumns implements RowPurger {
      private final int requiredColumns;

      private PurgeRowsWithoutRequiredColumns(int requiredColumns) {
         this.requiredColumns = requiredColumns;
      }

      public boolean shouldPurgeRow(LivenessInfo livenessInfo, Row.Deletion deletion, ColumnData[] data, int limit) {
         int actual = 0;

         for(int i = 0; i < limit && actual < this.requiredColumns; ++i) {
            ColumnData cd = data[i];
            if(cd.column.isRequiredForLiveness) {
               ++actual;
            }
         }

         return actual < this.requiredColumns;
      }

      public boolean hasLiveData(LivenessInfo livenessInfo, final int nowInSec, BiFunction<Boolean, BTree.ReduceFunction<Boolean, Cell>, Boolean> reduceCells) {
         final int[] requiredColumnCount = new int[1];
         Boolean hasDeadRequiredColumn = (Boolean)reduceCells.apply(Boolean.valueOf(false), new BTree.ReduceFunction<Boolean, Cell>() {
            public Boolean apply(Boolean ret, Cell cell) {
               if(cell.column.isRequiredForLiveness) {
                  ++requiredColumnCount[0];
                  if(!cell.isLive(nowInSec)) {
                     ret = Boolean.valueOf(true);
                  }
               }

               return ret;
            }

            public boolean stop(Boolean ret) {
               return ret.booleanValue();
            }
         });
         return !hasDeadRequiredColumn.booleanValue() && requiredColumnCount[0] == this.requiredColumns;
      }

      public Pair<Boolean, Integer> isAliveRowAndCountTombstones(Row row, int nowInSec) {
         int[] tombstones = new int[1];
         int[] actualRequiredColumns = new int[1];
         Boolean hasLiveCells = (Boolean)row.reduceCells(Boolean.FALSE, (ret, cell) -> {
            if(!cell.isLive(nowInSec)) {
               ++tombstones[0];
               return cell.column.isRequiredForLiveness?null:ret;
            } else {
               if(cell.column.isRequiredForLiveness) {
                  ++actualRequiredColumns[0];
               }

               return ret == null?ret:Boolean.TRUE;
            }
         });
         boolean isLiveRow = false;
         if(hasLiveCells != null && actualRequiredColumns[0] == this.requiredColumns && (hasLiveCells.booleanValue() || row.primaryKeyLivenessInfo().isLive(nowInSec))) {
            isLiveRow = true;
         } else if(!row.primaryKeyLivenessInfo().isLive(nowInSec) && row.hasDeletion(nowInSec) && tombstones[0] == 0) {
            ++tombstones[0];
         }

         return Pair.create(Boolean.valueOf(isLiveRow), Integer.valueOf(tombstones[0]));
      }
   }

   public static class PurgeRowsWithEmptyPrimaryKey extends RowPurger.PurgeEmptyRows {
      public PurgeRowsWithEmptyPrimaryKey() {
         super(true);
      }
   }

   public static class PurgeEmptyRows implements RowPurger {
      private final boolean requireLivePrimaryKey;

      protected PurgeEmptyRows(boolean requireLivePrimaryKey) {
         this.requireLivePrimaryKey = requireLivePrimaryKey;
      }

      protected PurgeEmptyRows() {
         this.requireLivePrimaryKey = false;
      }

      public boolean shouldPurgeRow(LivenessInfo livenessInfo, Row.Deletion deletion, ColumnData[] data, int limit) {
         return deletion.isLive() && livenessInfo.isEmpty() && (this.requireLivePrimaryKey || limit == 0);
      }

      public boolean hasLiveData(LivenessInfo livenessInfo, final int nowInSec, BiFunction<Boolean, BTree.ReduceFunction<Boolean, Cell>, Boolean> reduceCells) {
         return this.requireLivePrimaryKey?livenessInfo.isLive(nowInSec):(livenessInfo.isLive(nowInSec)?true:((Boolean)reduceCells.apply(Boolean.valueOf(false), new BTree.ReduceFunction<Boolean, Cell>() {
            public Boolean apply(Boolean ret, Cell cell) {
               return Boolean.valueOf(ret.booleanValue() || cell.isLive(nowInSec));
            }

            public boolean stop(Boolean ret) {
               return ret.booleanValue();
            }
         })).booleanValue());
      }

      public Pair<Boolean, Integer> isAliveRowAndCountTombstones(Row row, int nowInSec) {
         int[] tombstones = new int[1];
         Boolean hasLiveCells = (Boolean)row.reduceCells(Boolean.FALSE, (ret, cell) -> {
            if(!cell.isLive(nowInSec)) {
               ++tombstones[0];
               return ret;
            } else {
               return Boolean.TRUE;
            }
         });
         boolean isLiveRow = false;
         if((!hasLiveCells.booleanValue() || this.requireLivePrimaryKey) && !row.primaryKeyLivenessInfo().isLive(nowInSec)) {
            if(!row.primaryKeyLivenessInfo().isLive(nowInSec) && row.hasDeletion(nowInSec) && tombstones[0] == 0) {
               ++tombstones[0];
            }
         } else {
            isLiveRow = true;
         }

         return Pair.create(Boolean.valueOf(isLiveRow), Integer.valueOf(tombstones[0]));
      }
   }
}
