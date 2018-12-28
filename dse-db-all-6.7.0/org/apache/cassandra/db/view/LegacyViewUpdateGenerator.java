package org.apache.cassandra.db.view;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Iterator;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ViewColumnMetadata;

public class LegacyViewUpdateGenerator extends ViewUpdateGenerator {
   public LegacyViewUpdateGenerator(View view, DecoratedKey basePartitionKey, int nowInSec) {
      super(view, basePartitionKey, nowInSec, false);
   }

   protected ViewUpdateGenerator.UpdateAction updateAction(Row existingBaseRow, Row mergedBaseRow) {
      assert !mergedBaseRow.isEmpty();

      if(this.baseMetadata.isCompactTable()) {
         Clustering clustering = mergedBaseRow.clustering();

         for(int i = 0; i < clustering.size(); ++i) {
            if(clustering.get(i) == null) {
               return ViewUpdateGenerator.UpdateAction.NONE;
            }
         }
      }

      assert this.viewMetadata.getRegularBaseColumnsInViewPrimaryKey().size() <= 1 : "We currently only support one base non-PK column in the view PK";

      if(!this.viewMetadata.hasSamePrimaryKeyColumnsAsBaseTable()) {
         ColumnMetadata baseColumn = (ColumnMetadata)this.viewMetadata.getRegularBaseColumnsInViewPrimaryKey().iterator().next();

         assert !baseColumn.isComplex() : "A complex column couldn't be part of the view PK";

         Cell before = existingBaseRow == null?null:existingBaseRow.getCell(baseColumn);
         Cell after = mergedBaseRow.getCell(baseColumn);
         return before == after?(this.isLive(before)?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.NONE):(!this.isLive(before)?(this.isLive(after)?ViewUpdateGenerator.UpdateAction.NEW_ENTRY:ViewUpdateGenerator.UpdateAction.NONE):(!this.isLive(after)?ViewUpdateGenerator.UpdateAction.DELETE_OLD:(baseColumn.cellValueType().compare(before.value(), after.value()) == 0?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.SWITCH_ENTRY)));
      } else {
         boolean existingHasLiveData = existingBaseRow != null && existingBaseRow.hasLiveData(this.nowInSec, this.baseRowPurger);
         boolean mergedHasLiveData = mergedBaseRow.hasLiveData(this.nowInSec, this.baseRowPurger);
         return existingHasLiveData?(mergedHasLiveData?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.DELETE_OLD):(mergedHasLiveData?ViewUpdateGenerator.UpdateAction.NEW_ENTRY:ViewUpdateGenerator.UpdateAction.NONE);
      }
   }

   protected void addCell(ViewColumnMetadata viewColumn, Cell baseTableCell) {
      assert !viewColumn.isPrimaryKeyColumn();

      this.currentViewEntryBuilder.addCell(viewColumn.createCell(baseTableCell));
   }

   protected PartitionUpdate createEntryInternal(Row baseRow) {
      this.startNewUpdate(baseRow);
      this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(this.computeLivenessInfoForEntry(baseRow));
      this.currentViewEntryBuilder.addRowDeletion(baseRow.deletion());
      Iterator var2 = baseRow.iterator();

      while(var2.hasNext()) {
         ColumnData data = (ColumnData)var2.next();
         ViewColumnMetadata viewColumn = this.viewMetadata.getColumn(data.column().name);
         if(this.isSelected(viewColumn) && !viewColumn.isPrimaryKeyColumn()) {
            this.addColumnData(viewColumn, data);
         }
      }

      return this.submitUpdate();
   }

   protected PartitionUpdate updateEntryInternal(Row existingBaseRow, Row mergedBaseRow) {
      this.startNewUpdate(mergedBaseRow);
      this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(this.computeLivenessInfoForEntry(mergedBaseRow));
      this.currentViewEntryBuilder.addRowDeletion(mergedBaseRow.deletion());
      this.addDifferentCells(existingBaseRow, mergedBaseRow);
      return this.submitUpdate();
   }

   private void addDifferentCells(Row existingBaseRow, Row mergedBaseRow) {
      PeekingIterator<ColumnData> existingIter = Iterators.peekingIterator(existingBaseRow.iterator());
      Iterator var4 = mergedBaseRow.iterator();

      while(true) {
         while(true) {
            ColumnData mergedData;
            ColumnMetadata baseColumn;
            ViewColumnMetadata viewColumn;
            do {
               do {
                  if(!var4.hasNext()) {
                     return;
                  }

                  mergedData = (ColumnData)var4.next();
                  baseColumn = mergedData.column();
                  viewColumn = this.viewMetadata.getColumn(baseColumn.name);
               } while(!this.isSelected(viewColumn));
            } while(viewColumn.isPrimaryKeyColumn());

            ColumnData existingData = null;

            while(existingIter.hasNext()) {
               int cmp = baseColumn.compareTo(((ColumnData)existingIter.peek()).column());
               if(cmp < 0) {
                  break;
               }

               ColumnData next = (ColumnData)existingIter.next();
               if(cmp == 0) {
                  existingData = next;
                  break;
               }
            }

            if(existingData == null) {
               this.addColumnData(viewColumn, mergedData);
            } else if(mergedData != existingData) {
               if(!baseColumn.isComplex()) {
                  this.addCell(viewColumn, (Cell)mergedData);
               } else {
                  ComplexColumnData mergedComplexData = (ComplexColumnData)mergedData;
                  ComplexColumnData existingComplexData = (ComplexColumnData)existingData;
                  if(mergedComplexData.complexDeletion().supersedes(existingComplexData.complexDeletion())) {
                     this.currentViewEntryBuilder.addComplexDeletion(viewColumn.getPhysicalColumn(), mergedComplexData.complexDeletion());
                  }

                  PeekingIterator<Cell> existingCells = Iterators.peekingIterator(existingComplexData.iterator());
                  Iterator var12 = mergedComplexData.iterator();

                  while(var12.hasNext()) {
                     Cell mergedCell = (Cell)var12.next();
                     Cell existingCell = null;

                     while(existingCells.hasNext()) {
                        int cmp = baseColumn.cellPathComparator().compare(mergedCell.path(), ((Cell)existingCells.peek()).path());
                        if(cmp > 0) {
                           break;
                        }

                        Cell next = (Cell)existingCells.next();
                        if(cmp == 0) {
                           existingCell = next;
                           break;
                        }
                     }

                     if(mergedCell != existingCell) {
                        this.addCell(viewColumn, mergedCell);
                     }
                  }
               }
            }
         }
      }
   }

   protected PartitionUpdate deleteOldEntryInternal(Row existingBaseRow, Row mergedBaseRow) {
      this.startNewUpdate(existingBaseRow);
      long timestamp = this.computeTimestampForEntryDeletion(existingBaseRow, mergedBaseRow);
      long rowDeletion = mergedBaseRow.deletion().time().markedForDeleteAt();

      assert timestamp >= rowDeletion;

      if(timestamp > rowDeletion) {
         LivenessInfo info = LivenessInfo.withExpirationTime(timestamp, 2147483647, this.nowInSec);
         this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(info);
      }

      this.currentViewEntryBuilder.addRowDeletion(mergedBaseRow.deletion());
      this.addDifferentCells(existingBaseRow, mergedBaseRow);
      return this.submitUpdate();
   }

   private LivenessInfo computeLivenessInfoForEntry(Row baseRow) {
      assert this.viewMetadata.getRegularBaseColumnsInViewPrimaryKey().size() <= 1;

      LivenessInfo baseLiveness = baseRow.primaryKeyLivenessInfo();
      if(this.viewMetadata.hasSamePrimaryKeyColumnsAsBaseTable()) {
         if(this.viewMetadata.includeAllColumns) {
            return baseLiveness;
         } else {
            long timestamp = baseLiveness.timestamp();
            boolean hasNonExpiringLiveCell = false;
            Cell biggestExpirationCell = null;
            Iterator var7 = baseRow.cells().iterator();

            while(var7.hasNext()) {
               Cell cell = (Cell)var7.next();
               if(!this.isSelected(cell.column().name) && this.isLive(cell)) {
                  timestamp = Math.max(timestamp, cell.maxTimestamp());
                  if(!cell.isExpiring()) {
                     hasNonExpiringLiveCell = true;
                  } else if(biggestExpirationCell == null) {
                     biggestExpirationCell = cell;
                  } else if(cell.localDeletionTime() > biggestExpirationCell.localDeletionTime()) {
                     biggestExpirationCell = cell;
                  }
               }
            }

            if(baseLiveness.isLive(this.nowInSec) && !baseLiveness.isExpiring()) {
               return LivenessInfo.create(timestamp, this.nowInSec);
            } else if(hasNonExpiringLiveCell) {
               return LivenessInfo.create(timestamp, this.nowInSec);
            } else if(biggestExpirationCell == null) {
               return baseLiveness;
            } else if(biggestExpirationCell.localDeletionTime() <= baseLiveness.localExpirationTime() && baseLiveness.isLive(this.nowInSec)) {
               return baseLiveness;
            } else {
               return LivenessInfo.withExpirationTime(timestamp, biggestExpirationCell.ttl(), biggestExpirationCell.localDeletionTime());
            }
         }
      } else {
         Cell cell = baseRow.getCell((ColumnMetadata)this.viewMetadata.getRegularBaseColumnsInViewPrimaryKey().iterator().next());

         assert this.isLive(cell) : "We shouldn't have got there if the base row had no associated entry";

         return LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());
      }
   }

   private long computeTimestampForEntryDeletion(Row existingBaseRow, Row mergedBaseRow) {
      DeletionTime deletion = mergedBaseRow.deletion().time();
      if(this.viewMetadata.hasSamePrimaryKeyColumnsAsBaseTable()) {
         long timestamp = Math.max(deletion.markedForDeleteAt(), existingBaseRow.primaryKeyLivenessInfo().timestamp());
         if(this.view.getDefinition().includeAllColumns) {
            return timestamp;
         } else {
            Iterator var6 = existingBaseRow.cells().iterator();

            while(var6.hasNext()) {
               Cell cell = (Cell)var6.next();
               if(!this.isSelected(cell.column().name)) {
                  timestamp = Math.max(timestamp, cell.maxTimestamp());
               }
            }

            return timestamp;
         }
      } else {
         Cell before = existingBaseRow.getCell((ColumnMetadata)this.viewMetadata.getRegularBaseColumnsInViewPrimaryKey().iterator().next());

         assert this.isLive(before) : "We shouldn't have got there if the base row had no associated entry";

         return deletion.deletes(before)?deletion.markedForDeleteAt():before.timestamp();
      }
   }

   private boolean isSelected(ColumnIdentifier columnName) {
      ViewColumnMetadata viewColumn = this.viewMetadata.getColumn(columnName);
      return this.isSelected(viewColumn);
   }

   private boolean isSelected(ViewColumnMetadata viewColumn) {
      return viewColumn != null && viewColumn.isSelected();
   }
}
