package org.apache.cassandra.db.view;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ViewColumnMetadata;

public class NewViewUpdateGenerator extends ViewUpdateGenerator {
   NewViewUpdateGenerator(View view, DecoratedKey basePartitionKey, int nowInSec) {
      super(view, basePartitionKey, nowInSec, true);
   }

   protected ViewUpdateGenerator.UpdateAction updateAction(Row existingBaseRow, Row mergedBaseRow) {
      assert !mergedBaseRow.isEmpty();

      boolean hasExisting;
      if(!this.viewMetadata.hasSamePrimaryKeyColumnsAsBaseTable()) {
         hasExisting = true;
         Iterator var8 = this.viewMetadata.getRegularBaseColumnsInViewPrimaryKey().iterator();

         while(var8.hasNext()) {
            ColumnMetadata baseColumn = (ColumnMetadata)var8.next();

            assert !baseColumn.isComplex() : "A complex column couldn't be part of the view PK";

            Cell before = existingBaseRow == null?null:existingBaseRow.getCell(baseColumn);
            Cell after = mergedBaseRow.getCell(baseColumn);
            if(before == after && !this.isLive(before)) {
               return ViewUpdateGenerator.UpdateAction.NONE;
            }

            if(!this.isLive(before) && !this.isLive(after)) {
               return ViewUpdateGenerator.UpdateAction.NONE;
            }

            if(!this.isLive(after)) {
               return ViewUpdateGenerator.UpdateAction.DELETE_OLD;
            }

            if(before == null) {
               hasExisting = false;
            } else if(baseColumn.cellValueType().compare(before.value(), after.value()) != 0) {
               return ViewUpdateGenerator.UpdateAction.SWITCH_ENTRY;
            }
         }

         return hasExisting?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.NEW_ENTRY;
      } else {
         hasExisting = existingBaseRow != null && existingBaseRow.hasLiveData(this.nowInSec, this.baseRowPurger);
         boolean mergedHasLiveData = mergedBaseRow.hasLiveData(this.nowInSec, this.baseRowPurger);
         return hasExisting?(mergedHasLiveData?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.DELETE_OLD):(mergedHasLiveData?ViewUpdateGenerator.UpdateAction.NEW_ENTRY:ViewUpdateGenerator.UpdateAction.NONE);
      }
   }

   protected PartitionUpdate createEntryInternal(Row baseRow) {
      this.startNewUpdate(baseRow);
      this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(baseRow.primaryKeyLivenessInfo());
      this.currentViewEntryBuilder.addRowDeletion(baseRow.deletion());
      Iterator var2 = baseRow.iterator();

      while(true) {
         ColumnData data;
         ColumnMetadata baseColumn;
         ViewColumnMetadata viewColumn;
         do {
            do {
               if(!var2.hasNext()) {
                  return this.submitUpdate();
               }

               data = (ColumnData)var2.next();
               baseColumn = data.column();
               viewColumn = this.viewMetadata.getColumn(baseColumn.name);
            } while(viewColumn == null);
         } while(viewColumn.isPrimaryKeyColumn() && baseColumn.isPrimaryKeyColumn());

         this.addColumnData(viewColumn, data);
      }
   }

   protected PartitionUpdate updateEntryInternal(Row existingBaseRow, Row mergedBaseRow) {
      this.startNewUpdate(mergedBaseRow);
      this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(mergedBaseRow.primaryKeyLivenessInfo());
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
               } while(viewColumn == null);
            } while(viewColumn.isPrimaryKeyColumn() && baseColumn.isPrimaryKeyColumn());

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
            } else if(mergedData != existingData || viewColumn.isRequiredForLiveness()) {
               if(!baseColumn.isComplex()) {
                  Cell convertedBaseCell = this.maybeAsTombstone(viewColumn, (Cell)existingData, (Cell)mergedData);
                  this.currentViewEntryBuilder.addCell(viewColumn.createCell(convertedBaseCell));
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
                        this.currentViewEntryBuilder.addCell(viewColumn.createCell(mergedCell));
                     }
                  }
               }
            }
         }
      }
   }

   private Cell maybeAsTombstone(ViewColumnMetadata viewColumn, Cell existingBaseCell, Cell mergedBaseCell) {
      assert existingBaseCell != null && mergedBaseCell != null;

      return (Cell)((!viewColumn.isRegularBaseColumnInViewPrimaryKey() || Objects.equals(existingBaseCell.value(), mergedBaseCell.value())) && (!viewColumn.isFilteredNonPrimaryKey() || this.satisfyViewFilter(mergedBaseCell))?mergedBaseCell:BufferCell.tombstone(existingBaseCell.column(), existingBaseCell.timestamp(), this.nowInSec, existingBaseCell.path()));
   }

   private boolean satisfyViewFilter(Cell mergedBaseCell) {
      assert mergedBaseCell != null;

      List<RowFilter.Expression> expressions = this.view.getFilteredExpressions(mergedBaseCell.column());
      Iterator var3 = expressions.iterator();

      RowFilter.Expression e;
      do {
         if(!var3.hasNext()) {
            return true;
         }

         e = (RowFilter.Expression)var3.next();
      } while(e.operator().isSatisfiedBy(e.column().type, mergedBaseCell.value(), e.getIndexValue()));

      return false;
   }

   protected PartitionUpdate deleteOldEntryInternal(Row existingBaseRow, Row mergedBaseRow) {
      this.startNewUpdate(existingBaseRow);
      LivenessInfo info = mergedBaseRow.primaryKeyLivenessInfo();
      Row.Deletion deletion = mergedBaseRow.deletion();
      this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(info);
      this.currentViewEntryBuilder.addRowDeletion(deletion);
      this.addDifferentCells(existingBaseRow, mergedBaseRow);
      return this.submitUpdate();
   }
}
