package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

class PartitionWriter implements AutoCloseable {
   public int rowIndexCount;
   private final UnfilteredSerializer unfilteredSerializer;
   private final SerializationHeader header;
   private final SequentialWriter writer;
   private final Collection<SSTableFlushObserver> observers;
   private final RowIndexWriter rowTrie;
   private long initialPosition;
   private long startPosition;
   private int written;
   private long previousRowStart;
   private ClusteringPrefix firstClustering;
   private ClusteringPrefix lastClustering;
   private DeletionTime openMarker;
   private DeletionTime startOpenMarker;

   PartitionWriter(SerializationHeader header, ClusteringComparator comparator, SequentialWriter writer, SequentialWriter indexWriter, Version version, Collection<SSTableFlushObserver> observers) {
      this.openMarker = DeletionTime.LIVE;
      this.startOpenMarker = DeletionTime.LIVE;
      this.header = header;
      this.writer = writer;
      EncodingVersion version1 = version.encodingVersion();
      this.observers = observers;
      this.rowTrie = new RowIndexWriter(comparator, indexWriter);
      this.unfilteredSerializer = (UnfilteredSerializer)UnfilteredSerializer.serializers.get(version1);
   }

   public void reset() {
      this.initialPosition = this.writer.position();
      this.startPosition = -1L;
      this.previousRowStart = 0L;
      this.rowIndexCount = 0;
      this.written = 0;
      this.firstClustering = null;
      this.lastClustering = null;
      this.openMarker = DeletionTime.LIVE;
      this.rowTrie.reset();
   }

   public void close() {
      this.rowTrie.close();
   }

   public long writePartition(UnfilteredRowIterator iterator) throws IOException {
      this.writePartitionHeader(iterator);

      while(iterator.hasNext()) {
         this.add((Unfiltered)iterator.next());
      }

      return this.finish();
   }

   private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException {
      ByteBufferUtil.writeWithShortLength((ByteBuffer)iterator.partitionKey().getKey(), (DataOutputPlus)this.writer);
      long deletionTimePosition = this.writer.position();
      DeletionTime deletionTime = iterator.partitionLevelDeletion();
      DeletionTime.serializer.serialize((DeletionTime)deletionTime, this.writer);
      if(!this.observers.isEmpty()) {
         this.observers.forEach((o) -> {
            o.partitionLevelDeletion(deletionTime, deletionTimePosition);
         });
      }

      if(this.header.hasStatic()) {
         long staticRowPosition = this.writer.position();
         Row staticRow = iterator.staticRow();
         this.unfilteredSerializer.serializeStaticRow(staticRow, this.header, this.writer);
         if(!this.observers.isEmpty()) {
            this.observers.forEach((o) -> {
               o.staticRow(staticRow, staticRowPosition);
            });
         }
      }

   }

   private long currentPosition() {
      return this.writer.position() - this.initialPosition;
   }

   private void addIndexBlock() throws IOException {
      RowIndexReader.IndexInfo cIndexInfo = new RowIndexReader.IndexInfo(this.startPosition, this.startOpenMarker);
      this.rowTrie.add(this.firstClustering, this.lastClustering, cIndexInfo);
      this.firstClustering = null;
      ++this.rowIndexCount;
   }

   private void add(Unfiltered unfiltered) throws IOException {
      long pos = this.currentPosition();
      if(this.firstClustering == null) {
         this.firstClustering = unfiltered.clustering();
         this.startOpenMarker = this.openMarker;
         this.startPosition = pos;
      }

      long unfilteredPosition = this.writer.position();
      this.unfilteredSerializer.serialize((Unfiltered)unfiltered, this.header, this.writer, pos - this.previousRowStart);
      if(!this.observers.isEmpty()) {
         this.observers.forEach((o) -> {
            o.nextUnfilteredCluster(unfiltered, unfilteredPosition);
         });
      }

      this.lastClustering = unfiltered.clustering();
      this.previousRowStart = pos;
      ++this.written;
      if(unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER) {
         RangeTombstoneMarker marker = (RangeTombstoneMarker)unfiltered;
         this.openMarker = marker.isOpen(false)?marker.openDeletionTime(false):DeletionTime.LIVE;
      }

      if(this.currentPosition() - this.startPosition >= (long)DatabaseDescriptor.getColumnIndexSize()) {
         this.addIndexBlock();
      }

   }

   private long finish() throws IOException {
      long endPosition = this.currentPosition();
      this.unfilteredSerializer.writeEndOfPartition(this.writer);
      if(this.written == 0) {
         return -1L;
      } else {
         long trieRoot = -1L;
         if(this.firstClustering != null && this.rowIndexCount > 0) {
            this.addIndexBlock();
         }

         if(this.rowIndexCount > 1) {
            trieRoot = this.rowTrie.complete(endPosition);
         }

         return trieRoot;
      }
   }
}
