package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.reactivex.Completable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.RxThreads;

abstract class AbstractDataSet implements DataSet {
   private final TableMetadata metadata;
   private final NavigableMap<DecoratedKey, AbstractDataSet.DefaultPartition> partitions;
   private volatile long totalSize;
   private volatile int numberOfCells;

   public AbstractDataSet(TableMetadata metadata) {
      this.metadata = metadata;
      this.partitions = this.newNavigableMap(DecoratedKey.comparator);
   }

   protected abstract <K, V> NavigableMap<K, V> newNavigableMap(Comparator<? super K> var1);

   private boolean isBlocking() {
      return this.partitions instanceof ConcurrentMap;
   }

   public boolean isEmpty() {
      return this.partitions.isEmpty();
   }

   public Completable apply(PartitionUpdate update) {
      return this.isBlocking() && TPCUtils.isTPCThread()?RxThreads.subscribeOnIo(Completable.defer(() -> {
         return this.executeApply(update);
      }), TPCTaskType.WRITE_LOCAL):this.executeApply(update);
   }

   public Flow<DataSet.Partition> getPartition(DecoratedKey partitionKey) {
      DataSet.Partition partition = (DataSet.Partition)this.partitions.get(partitionKey);
      return partition == null?Flow.empty():Flow.just(partition);
   }

   public Flow<DataSet.Partition> getPartitions(DataRange range) {
      return this.isEmpty()?Flow.empty():Flow.fromIterator(this.getPartitionIterator(range));
   }

   private Completable executeApply(PartitionUpdate update) {
      DecoratedKey dk = update.partitionKey();
      AbstractDataSet.DefaultPartition partition = (AbstractDataSet.DefaultPartition)this.partitions.get(dk);
      RequestValidations.checkNotNull(partition, "INSERT are not supported on system views");
      partition.apply(update);
      return Completable.complete();
   }

   private Iterator<? extends DataSet.Partition> getPartitionIterator(final DataRange dataRange) {
      AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
      PartitionPosition startKey = (PartitionPosition)keyRange.left;
      PartitionPosition endKey = (PartitionPosition)keyRange.right;
      NavigableMap<DecoratedKey, AbstractDataSet.DefaultPartition> selection = this.partitions;
      if(startKey.isMinimum() && endKey.isMinimum()) {
         return selection.values().iterator();
      } else if(startKey.isMinimum() && endKey instanceof DecoratedKey) {
         return selection.headMap((DecoratedKey)endKey, keyRange.isEndInclusive()).values().iterator();
      } else if(startKey instanceof DecoratedKey && endKey instanceof DecoratedKey) {
         return selection.subMap((DecoratedKey)startKey, keyRange.isStartInclusive(), (DecoratedKey)endKey, keyRange.isEndInclusive()).values().iterator();
      } else {
         if(startKey instanceof DecoratedKey) {
            selection = selection.tailMap((DecoratedKey)startKey, keyRange.isStartInclusive());
         }

         if(endKey instanceof DecoratedKey) {
            selection = selection.headMap((DecoratedKey)endKey, keyRange.isEndInclusive());
         }

         final Iterator<AbstractDataSet.DefaultPartition> iterator = selection.values().iterator();
         return new AbstractIterator<AbstractDataSet.DefaultPartition>() {
            private boolean encounteredPartitionsWithinRange;

            protected AbstractDataSet.DefaultPartition computeNext() {
               while(true) {
                  if(iterator.hasNext()) {
                     AbstractDataSet.DefaultPartition partition = (AbstractDataSet.DefaultPartition)iterator.next();
                     if(dataRange.contains(partition.key())) {
                        this.encounteredPartitionsWithinRange = true;
                        return partition;
                     }

                     if(!this.encounteredPartitionsWithinRange) {
                        continue;
                     }

                     return (AbstractDataSet.DefaultPartition)this.endOfData();
                  }

                  return (AbstractDataSet.DefaultPartition)this.endOfData();
               }
            }
         };
      }
   }

   public DataSet.RowBuilder newRowBuilder(Object... clustering) {
      return new AbstractDataSet.DefaultRowBuilder(this.metadata, this.makeClustering(clustering));
   }

   public Completable addRow(Object partitionKey) {
      return this.isBlocking() && TPCUtils.isTPCThread()?RxThreads.subscribeOnIo(Completable.defer(() -> {
         return this.executeAddRow(partitionKey);
      }), TPCTaskType.POPULATE_VIRTUAL_TABLE):this.executeAddRow(partitionKey);
   }

   private Completable executeAddRow(Object partitionKey) {
      AbstractDataSet.DefaultPartition partition = this.createPartitionIfAbsent(partitionKey);
      partition.add(new AbstractDataSet.DefaultRow(this.metadata, Clustering.EMPTY, ImmutableMap.of()));
      return Completable.complete();
   }

   private AbstractDataSet.DefaultPartition createPartitionIfAbsent(Object partitionKey) {
      DecoratedKey dk = this.makeDecoratedKey(new Object[]{partitionKey});
      return (AbstractDataSet.DefaultPartition)this.partitions.computeIfAbsent(dk, (key) -> {
         return new AbstractDataSet.DefaultPartition(dk, this.newNavigableMap(this.metadata.comparator));
      });
   }

   public Completable addRow(Object partitionKey, DataSet.RowBuilder builder) {
      return this.isBlocking() && TPCUtils.isTPCThread()?RxThreads.subscribeOnIo(Completable.defer(() -> {
         return this.executeAddRow(partitionKey, builder);
      }), TPCTaskType.POPULATE_VIRTUAL_TABLE):this.executeAddRow(partitionKey, builder);
   }

   private Completable executeAddRow(Object partitionKey, DataSet.RowBuilder builder) {
      AbstractDataSet.DefaultPartition partition = this.createPartitionIfAbsent(partitionKey);
      AbstractDataSet.DefaultRow row = ((AbstractDataSet.DefaultRowBuilder)builder).build();
      partition.add(row);
      this.numberOfCells += row.numberOfCells();
      this.totalSize += row.sizeInBytes();
      return Completable.complete();
   }

   private DecoratedKey makeDecoratedKey(Object... partitionKeyValues) {
      ByteBuffer partitionKey = partitionKeyValues.length == 1?decompose(this.metadata.partitionKeyType, partitionKeyValues[0]):((CompositeType)this.metadata.partitionKeyType).decompose(partitionKeyValues);
      return this.metadata.partitioner.decorateKey(partitionKey);
   }

   private Clustering makeClustering(Object... elements) {
      UnmodifiableArrayList<ColumnMetadata> clusteringColumns = this.metadata.clusteringColumns();
      if(clusteringColumns.size() != elements.length) {
         throw new IllegalArgumentException();
      } else if(elements.length == 0) {
         return Clustering.EMPTY;
      } else {
         ByteBuffer[] clusteringByteBuffers = new ByteBuffer[elements.length];

         for(int i = 0; i < elements.length; ++i) {
            clusteringByteBuffers[i] = decompose(((ColumnMetadata)clusteringColumns.get(i)).type, elements[i]);
         }

         return Clustering.make(clusteringByteBuffers);
      }
   }

   private static <T> ByteBuffer decompose(AbstractType<?> type, T value) {
      return ((AbstractType<T>)type).decompose(value);
   }

   private static <T> T compose(AbstractType<?> type, ByteBuffer bytes) {
      return (T)type.compose(bytes);
   }

   public int getAverageColumnSize() {
      return (int)this.totalSize / this.numberOfCells;
   }

   private static class DataWrapper {
      private final ColumnMetadata metadata;
      private final Supplier<?> supplier;
      private final Consumer<?> consumer;

      private DataWrapper(ColumnMetadata metadata, Supplier<?> supplier, Consumer<?> consumer) {
         this.metadata = metadata;
         this.supplier = supplier;
         this.consumer = consumer;
      }

      public boolean isMutable() {
         return this.consumer != null;
      }

      private ColumnMetadata column() {
         return this.metadata;
      }

      public Cell getCell(long now) {
         return BufferCell.live(this.column(), now, AbstractDataSet.decompose(this.column().type, this.supplier.get()));
      }

      public void setCell(Cell cell) {
         assert (this.consumer != null);
         this.consumer.accept(AbstractDataSet.compose(this.column().type, cell.value()));
      }

      public long sizeInBytes() {
         final int fixedLength = this.metadata.type.valueLengthIfFixed();
         return (fixedLength > 0) ? fixedLength : ((long)decompose(this.column().type, this.supplier.get()).remaining());
      }
   }

   private static class DefaultRowBuilder implements DataSet.RowBuilder {
      private final TableMetadata metadata;
      private final Clustering clustering;
      private final Builder<ColumnMetadata, AbstractDataSet.DataWrapper> builder = ImmutableMap.builder();

      public DefaultRowBuilder(TableMetadata metadata, Clustering clustering) {
         this.metadata = metadata;
         this.clustering = clustering;
      }

      public DataSet.RowBuilder addColumn(String columnName, Supplier<?> supplier) {
         return this.addColumn(columnName, supplier, (Consumer)null);
      }

      public DataSet.RowBuilder addColumn(String columnName, Supplier<?> supplier, Consumer<?> consumer) {
         ColumnMetadata column = this.metadata.getColumn(ByteBufferUtil.bytes(columnName));
         if(null != column && column.isRegular()) {
            this.builder.put(column, new AbstractDataSet.DataWrapper(column, supplier, consumer));
            return this;
         } else {
            throw new IllegalArgumentException();
         }
      }

      public AbstractDataSet.DefaultRow build() {
         return new AbstractDataSet.DefaultRow(this.metadata, this.clustering, this.builder.build());
      }
   }

   public static class DefaultRow {
      private final TableMetadata metadata;
      private final Clustering clustering;
      private final Map<ColumnMetadata, AbstractDataSet.DataWrapper> cells;

      private DefaultRow(TableMetadata metadata, Clustering clustering, ImmutableMap<ColumnMetadata, AbstractDataSet.DataWrapper> cells) {
         this.metadata = metadata;
         this.clustering = clustering;
         this.cells = cells;
      }

      public Clustering clustering() {
         return this.clustering;
      }

      public void update(DecoratedKey pk, Iterable<Cell> newValues) {
         Iterator var3 = newValues.iterator();

         Cell newValue;
         while(var3.hasNext()) {
            newValue = (Cell)var3.next();
            ColumnMetadata column = newValue.column();
            AbstractDataSet.DataWrapper wrapper = (AbstractDataSet.DataWrapper)this.cells.get(column);
            if(wrapper == null) {
               throw RequestValidations.invalidRequest("Insert operations are not supported on column %s of system view=%s.%s, partition key=%s %s", new Object[]{column, column.ksName, column.cfName, this.metadata.partitionKeyType.getString(pk.getKey()), this.clustering == Clustering.EMPTY?"":", row=" + this.clustering.toString(this.metadata)});
            }

            if(!wrapper.isMutable()) {
               throw RequestValidations.invalidRequest("Modifications are not supported on column %s of system view=%s.%s, partition key=%s %s", new Object[]{column, column.ksName, column.cfName, this.metadata.partitionKeyType.getString(pk.getKey()), this.clustering == Clustering.EMPTY?"":", row=" + this.clustering.toString(this.metadata)});
            }
         }

         var3 = newValues.iterator();

         while(var3.hasNext()) {
            newValue = (Cell)var3.next();
            AbstractDataSet.DataWrapper wrapper = (AbstractDataSet.DataWrapper)this.cells.get(newValue.column());
            wrapper.setCell(newValue);
         }

      }

      public Row toTableRow(RegularAndStaticColumns columns, long now) {
         ColumnData[] data = this.getColumnData(columns, now);
         return ArrayBackedRow.create(this.clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE, data, data.length);
      }

      private ColumnData[] getColumnData(RegularAndStaticColumns columns, long now) {
         if(columns.isEmpty()) {
            return new ColumnData[0];
         } else {
            ColumnData[] data = new ColumnData[columns.size()];
            int i = 0;

            AbstractDataSet.DataWrapper wrapper;
            for(Iterator iter = columns.selectOrderIterator(); iter.hasNext(); data[i++] = wrapper == null?null:wrapper.getCell(now)) {
               ColumnMetadata metadata = (ColumnMetadata)iter.next();
               wrapper = (AbstractDataSet.DataWrapper)this.cells.get(metadata);
            }

            return data;
         }
      }

      public long sizeInBytes() {
         int size = 0;

         AbstractDataSet.DataWrapper cell;
         for(Iterator var2 = this.cells.values().iterator(); var2.hasNext(); size = (int)((long)size + cell.sizeInBytes())) {
            cell = (AbstractDataSet.DataWrapper)var2.next();
         }

         return (long)size;
      }

      public int numberOfCells() {
         return this.cells.size();
      }
   }

   private static final class DefaultPartition implements DataSet.Partition {
      private final DecoratedKey key;
      private final NavigableMap<Clustering, AbstractDataSet.DefaultRow> rows;

      private DefaultPartition(DecoratedKey key, NavigableMap<Clustering, AbstractDataSet.DefaultRow> rows) {
         this.key = key;
         this.rows = rows;
      }

      public void add(AbstractDataSet.DefaultRow row) {
         this.rows.put(row.clustering(), row);
      }

      public DecoratedKey key() {
         return this.key;
      }

      public void apply(PartitionUpdate update) {
         Iterator var2 = update.iterator();

         while(var2.hasNext()) {
            Row r = (Row)var2.next();
            AbstractDataSet.DefaultRow row = (AbstractDataSet.DefaultRow)this.rows.get(r.clustering());
            RequestValidations.checkNotNull(row, "INSERT are not supported on system views");
            row.update(update.partitionKey(), r.cells());
         }

      }

      public FlowableUnfilteredPartition toFlowable(TableMetadata metadata, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter, long now) {
         RegularAndStaticColumns columns = columnFilter.queriedColumns();
         Flow<Unfiltered> rows = this.getRows(clusteringIndexFilter, columns, now);
         return this.newFlowableUnfilteredPartition(metadata, this.key, rows, columns);
      }

      private FlowableUnfilteredPartition newFlowableUnfilteredPartition(TableMetadata metadata, DecoratedKey partitionKey, Flow<Unfiltered> rows, RegularAndStaticColumns columns) {
         PartitionHeader header = this.newPartitionHeader(metadata, partitionKey, columns);
         return FlowableUnfilteredPartition.create(header, Rows.EMPTY_STATIC_ROW, rows);
      }

      private PartitionHeader newPartitionHeader(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns) {
         return new PartitionHeader(metadata, partitionKey, DeletionTime.LIVE, columns, false, EncodingStats.NO_STATS);
      }

      protected Flow<Unfiltered> getRows(final ClusteringIndexFilter clusteringIndexFilter, final RegularAndStaticColumns columns, final long now) {
         NavigableMap<Clustering, AbstractDataSet.DefaultRow> partitionRows = clusteringIndexFilter.isReversed()?this.rows.descendingMap():this.rows;
         final Iterator<AbstractDataSet.DefaultRow> rowIterator = partitionRows.values().iterator();
         return Flow.fromIterator(new AbstractIterator<Unfiltered>() {
            protected Unfiltered computeNext() {
               while(true) {
                  if(rowIterator.hasNext()) {
                     AbstractDataSet.DefaultRow next = (AbstractDataSet.DefaultRow)rowIterator.next();
                     if(!clusteringIndexFilter.selects(next.clustering())) {
                        continue;
                     }

                     return next.toTableRow(columns, now);
                  }

                  return (Unfiltered)this.endOfData();
               }
            }
         });
      }
   }
}
