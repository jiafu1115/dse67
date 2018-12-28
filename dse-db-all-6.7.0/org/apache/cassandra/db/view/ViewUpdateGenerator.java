package org.apache.cassandra.db.view;

import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowPurger;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewColumnMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.utils.flow.Flow;

public abstract class ViewUpdateGenerator {
   protected final View view;
   protected final int nowInSec;
   protected final TableMetadata baseMetadata;
   protected final DecoratedKey baseDecoratedKey;
   protected final ByteBuffer[] basePartitionKey;
   protected final ViewMetadata viewMetadata;
   protected final RowPurger baseRowPurger;
   protected final ByteBuffer[] currentViewEntryPartitionKey;
   protected final Row.Builder currentViewEntryBuilder;

   public static ViewUpdateGenerator getGenerator(View view, DecoratedKey basePartitionKey, int nowInSec) {
      return (ViewUpdateGenerator)(view.getDefinition().isLegacyView()?new LegacyViewUpdateGenerator(view, basePartitionKey, nowInSec):new NewViewUpdateGenerator(view, basePartitionKey, nowInSec));
   }

   public ViewUpdateGenerator(View view, DecoratedKey basePartitionKey, int nowInSec, boolean isNewView) {
      this.nowInSec = nowInSec;
      this.view = view;
      this.baseMetadata = view.getDefinition().baseTableMetadata;
      this.baseRowPurger = this.baseMetadata.rowPurger();
      this.baseDecoratedKey = basePartitionKey;
      this.basePartitionKey = extractKeyComponents(basePartitionKey, this.baseMetadata.partitionKeyType);
      this.viewMetadata = view.getDefinition();
      this.currentViewEntryPartitionKey = new ByteBuffer[this.viewMetadata.partitionKeyColumns().size()];
      this.currentViewEntryBuilder = isNewView?Row.Builder.unsorted(nowInSec):Row.Builder.sorted();
   }

   protected abstract ViewUpdateGenerator.UpdateAction updateAction(Row var1, Row var2);

   protected abstract PartitionUpdate updateEntryInternal(Row var1, Row var2);

   protected abstract PartitionUpdate deleteOldEntryInternal(Row var1, Row var2);

   protected abstract PartitionUpdate createEntryInternal(Row var1);

   protected void addCell(ViewColumnMetadata viewColumn, Cell baseTableCell) {
      this.currentViewEntryBuilder.addCell(viewColumn.createCell(baseTableCell));
   }

   protected static ByteBuffer[] extractKeyComponents(DecoratedKey partitionKey, AbstractType<?> type) {
      return type instanceof CompositeType?((CompositeType)type).split(partitionKey.getKey()):new ByteBuffer[]{partitionKey.getKey()};
   }

   protected Flow<PartitionUpdate> createViewUpdates(Row existingBaseRow, Row mergedBaseRow) {
      Flow flowableViewUpdates;
      switch(null.$SwitchMap$org$apache$cassandra$db$view$ViewUpdateGenerator$UpdateAction[this.updateAction(existingBaseRow, mergedBaseRow).ordinal()]) {
      case 1:
         flowableViewUpdates = Flow.empty();
         break;
      case 2:
         flowableViewUpdates = this.createEntry(mergedBaseRow);
         break;
      case 3:
         flowableViewUpdates = this.deleteOldEntry(existingBaseRow, mergedBaseRow);
         break;
      case 4:
         flowableViewUpdates = this.updateEntry(existingBaseRow, mergedBaseRow);
         break;
      case 5:
         flowableViewUpdates = Flow.concat(new Flow[]{this.createEntry(mergedBaseRow), this.deleteOldEntry(existingBaseRow, mergedBaseRow)});
         break;
      default:
         throw new AssertionError();
      }

      return flowableViewUpdates;
   }

   protected final Flow<Boolean> matchesViewFilter(Row baseRow) {
      return this.view.matchesViewFilter(this.baseDecoratedKey, baseRow, this.nowInSec);
   }

   protected final boolean isLive(Cell cell) {
      return cell != null && cell.isLive(this.nowInSec);
   }

   protected Flow<PartitionUpdate> createEntry(Row baseRow) {
      return this.matchesViewFilter(baseRow).skippingMap((f) -> {
         return !f.booleanValue()?null:this.createEntryInternal(baseRow);
      });
   }

   private Flow<PartitionUpdate> updateEntry(Row existingBaseRow, Row mergedBaseRow) {
      return this.matchesViewFilter(existingBaseRow).flatMap((f) -> {
         return !f.booleanValue()?this.createEntry(mergedBaseRow):this.matchesViewFilter(mergedBaseRow).skippingMap((s) -> {
            return !s.booleanValue()?this.deleteOldEntryInternal(existingBaseRow, mergedBaseRow):this.updateEntryInternal(existingBaseRow, mergedBaseRow);
         });
      });
   }

   private Flow<PartitionUpdate> deleteOldEntry(Row existingBaseRow, Row mergedBaseRow) {
      return this.matchesViewFilter(existingBaseRow).skippingMap((f) -> {
         return !f.booleanValue()?null:this.deleteOldEntryInternal(existingBaseRow, mergedBaseRow);
      });
   }

   protected final void startNewUpdate(Row baseRow) {
      ByteBuffer[] clusteringValues = new ByteBuffer[this.viewMetadata.clusteringColumns().size()];
      Iterator var3 = this.viewMetadata.primaryKeyColumns().iterator();

      while(var3.hasNext()) {
         ColumnMetadata viewPrimaryKey = (ColumnMetadata)var3.next();
         ColumnMetadata baseColumn = this.getBaseColumn(viewPrimaryKey);
         ByteBuffer value = this.getValueForPK(baseColumn, baseRow);
         if(viewPrimaryKey.isPartitionKey()) {
            this.currentViewEntryPartitionKey[viewPrimaryKey.position()] = value;
         } else {
            clusteringValues[viewPrimaryKey.position()] = value;
         }
      }

      this.currentViewEntryBuilder.newRow(Clustering.make(clusteringValues));
   }

   protected final void addColumnData(ViewColumnMetadata viewColumn, ColumnData baseTableData) {
      assert viewColumn.isComplex() == baseTableData.column().isComplex();

      if(!viewColumn.isComplex()) {
         this.addCell(viewColumn, (Cell)baseTableData);
      } else {
         ComplexColumnData complexData = (ComplexColumnData)baseTableData;
         this.currentViewEntryBuilder.addComplexDeletion(viewColumn.getPhysicalColumn(), complexData.complexDeletion());
         Iterator var4 = complexData.iterator();

         while(var4.hasNext()) {
            Cell cell = (Cell)var4.next();
            this.addCell(viewColumn, cell);
         }

      }
   }

   protected final PartitionUpdate submitUpdate() {
      Row row = this.currentViewEntryBuilder.build();
      if(row.isEmpty()) {
         return null;
      } else {
         DecoratedKey partitionKey = this.makeCurrentPartitionKey();
         PartitionUpdate update = this.viewMetadata.createUpdate(partitionKey);
         update.add(row);
         return update;
      }
   }

   private DecoratedKey makeCurrentPartitionKey() {
      ByteBuffer rawKey = this.viewMetadata.partitionKeyColumns().size() == 1?this.currentViewEntryPartitionKey[0]:CompositeType.build(this.currentViewEntryPartitionKey);
      return this.baseDecoratedKey.getPartitioner().decorateKey(rawKey);
   }

   private ByteBuffer getValueForPK(ColumnMetadata column, Row row) {
      switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[column.kind.ordinal()]) {
      case 1:
         return this.basePartitionKey[column.position()];
      case 2:
         return row.clustering().get(column.position());
      default:
         return row.getCell(column).value();
      }
   }

   public ColumnMetadata getBaseColumn(ColumnMetadata viewColumn) {
      ColumnMetadata baseColumn = this.baseMetadata.getColumn(viewColumn.name);

      assert baseColumn != null;

      return baseColumn;
   }

   protected static enum UpdateAction {
      NONE,
      NEW_ENTRY,
      DELETE_OLD,
      UPDATE_EXISTING,
      SWITCH_ENTRY;

      private UpdateAction() {
      }
   }
}
