package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.IndexFileEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableScanner;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TrieIndexSSTableReader extends SSTableReader {
   private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableReader.class);
   protected FileHandle rowIndexFile;
   protected PartitionIndex partitionIndex;

   TrieIndexSSTableReader(Descriptor desc, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
      super(desc, components, metadata, maxDataAge.longValue(), sstableMetadata, openReason, header);
   }

   protected void loadIndex(final boolean preload) throws IOException {
      if (this.components.contains(Component.PARTITION_INDEX)) {
         try (final FileHandle.Builder rowIndexBuilder = this.indexFileHandleBuilder(Component.ROW_INDEX);
              final FileHandle.Builder partitionIndexBuilder = this.indexFileHandleBuilder(Component.PARTITION_INDEX)) {
            this.rowIndexFile = rowIndexBuilder.complete();
            this.partitionIndex = PartitionIndex.load(partitionIndexBuilder, this.metadata().partitioner, preload);
            this.first = this.partitionIndex.firstKey();
            this.last = this.partitionIndex.lastKey();
         }
      }
   }

   protected void releaseIndex() {
      if(this.rowIndexFile != null) {
         this.rowIndexFile.close();
         this.rowIndexFile = null;
      }

      if(this.partitionIndex != null) {
         this.partitionIndex.close();
         this.partitionIndex = null;
      }

   }

   protected SSTableReader clone(SSTableReader.OpenReason reason) {
      TrieIndexSSTableReader replacement = internalOpen(this.descriptor, this.components, this.metadata, this.rowIndexFile.sharedCopy(), this.dataFile.sharedCopy(), this.partitionIndex.sharedCopy(), this.bf.sharedCopy(), this.maxDataAge, this.sstableMetadata, reason, this.header);
      replacement.first = this.first;
      replacement.last = this.last;
      replacement.isSuspect.set(this.isSuspect.get());
      return replacement;
   }

   static TrieIndexSSTableReader internalOpen(Descriptor desc, Set<Component> components, TableMetadataRef metadata, FileHandle ifile, FileHandle dfile, PartitionIndex partitionIndex, IFilter bf, long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header) {
      assert desc != null && ifile != null && dfile != null && partitionIndex != null && bf != null && sstableMetadata != null;

      assert desc.getFormat() == TrieIndexFormat.instance;

      TrieIndexSSTableReader reader = TrieIndexFormat.readerFactory.open(desc, components, metadata, Long.valueOf(maxDataAge), sstableMetadata, openReason, header);
      reader.bf = bf;
      reader.rowIndexFile = ifile;
      reader.dataFile = dfile;
      reader.partitionIndex = partitionIndex;
      reader.setup(true);
      return reader;
   }

   protected void setup(boolean trackHotness) {
      super.setup(trackHotness);
      this.tidy.addCloseable(this.partitionIndex);
      this.tidy.addCloseable(this.rowIndexFile);
   }

   public void addTo(Ref.IdentityCollection identities) {
      super.addTo(identities);
      this.rowIndexFile.addTo(identities);
      this.partitionIndex.addTo(identities);
   }

   public long estimatedKeys() {
      return this.partitionIndex.size();
   }

   public SSTableReader.PartitionReader reader(FileDataInput file, boolean shouldCloseFile, RowIndexEntry indexEntry, SerializationHelper helper, Slices slices, boolean reversed, Rebufferer.ReaderConstraint readerConstraint) throws IOException {
      return (SSTableReader.PartitionReader)(indexEntry.isIndexed()?(reversed?new ReverseIndexedReader(this, (TrieIndexEntry)indexEntry, slices, file, shouldCloseFile, helper, readerConstraint):new ForwardIndexedReader(this, (TrieIndexEntry)indexEntry, slices, file, shouldCloseFile, helper, readerConstraint)):(reversed?new ReverseReader(this, slices, file, shouldCloseFile, helper):new ForwardReader(this, slices, file, shouldCloseFile, helper)));
   }

   public RowIndexEntry getPosition(PartitionPosition key, SSTableReader.Operator op, SSTableReadsListener listener, Rebufferer.ReaderConstraint rc) {
      if(op == SSTableReader.Operator.EQ) {
         return this.getExactPosition((DecoratedKey)key, listener, rc);
      } else if(this.filterLast() && this.last.compareTo(key) < 0) {
         return null;
      } else {
         boolean filteredLeft = this.filterFirst() && this.first.compareTo(key) > 0;
         PartitionPosition searchKey = filteredLeft?this.first:key;
         SSTableReader.Operator searchOp = filteredLeft?SSTableReader.Operator.GE:op;

         try {
            PartitionIndex.Reader reader = this.partitionIndex.openReader(rc);
            Throwable var9 = null;

            RowIndexEntry var10;
            try {
               var10 = (RowIndexEntry)reader.ceiling((PartitionPosition)searchKey, (pos, assumeGreater, compareKey) -> {
                  return this.retrieveEntryIfAcceptable(searchOp, compareKey, pos, assumeGreater, rc);
               });
            } catch (Throwable var20) {
               var9 = var20;
               throw var20;
            } finally {
               if(reader != null) {
                  if(var9 != null) {
                     try {
                        reader.close();
                     } catch (Throwable var19) {
                        var9.addSuppressed(var19);
                     }
                  } else {
                     reader.close();
                  }
               }

            }

            return var10;
         } catch (IOException var22) {
            this.markSuspect();
            throw new CorruptSSTableException(var22, this.rowIndexFile.path());
         }
      }
   }


   private RowIndexEntry retrieveEntryIfAcceptable(final Operator searchOp, final PartitionPosition searchKey, long pos, final boolean assumeGreater, final Rebufferer.ReaderConstraint rc) throws IOException {
      if (pos >= 0L) {
         try (final FileDataInput in = this.rowIndexFile.createReader(pos, rc)) {
            if (assumeGreater) {
               ByteBufferUtil.skipShortLength(in);
            }
            else {
               final ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
               final DecoratedKey decorated = this.decorateKey(indexKey);
               if (searchOp.apply(decorated.compareTo(searchKey)) != 0) {
                  return null;
               }
            }
            return TrieIndexEntry.deserialize(in, in.getFilePointer());
         }
      }
      pos ^= -1L;
      if (!assumeGreater) {
         try (final FileDataInput in = this.dataFile.createReader(pos, rc)) {
            final ByteBuffer indexKey = ByteBufferUtil.readWithShortLength(in);
            final DecoratedKey decorated = this.decorateKey(indexKey);
            if (searchOp.apply(decorated.compareTo(searchKey)) != 0) {
               return null;
            }
         }
      }
      return new RowIndexEntry(pos);
   }


   public boolean contains(final DecoratedKey dk, final Rebufferer.ReaderConstraint rc) {
      if (!this.bf.isPresent(dk)) {
         return false;
      }
      if (this.filterFirst() && this.first.compareTo(dk) > 0) {
         return false;
      }
      if (this.filterLast() && this.last.compareTo(dk) < 0) {
         return false;
      }
      try (final PartitionIndex.Reader reader = this.partitionIndex.openReader(rc)) {
         final long indexPos = reader.exactCandidate(dk);
         if (indexPos == Long.MIN_VALUE) {
            return false;
         }
         try (final FileDataInput in = this.createIndexOrDataReader(indexPos, rc)) {
            return ByteBufferUtil.equalsWithShortLength(in, dk.getKey());
         }
      }
      catch (IOException e) {
         this.markSuspect();
         throw new CorruptSSTableException(e, this.rowIndexFile.path());
      }
   }

   FileDataInput createIndexOrDataReader(long indexPos, Rebufferer.ReaderConstraint rc) {
      return indexPos >= 0L?this.rowIndexFile.createReader(indexPos, rc):this.dataFile.createReader(~indexPos, rc);
   }

   public DecoratedKey keyAt(final long dataPosition, final Rebufferer.ReaderConstraint rc) throws IOException {
      DecoratedKey key;
      try (final FileDataInput in = this.dataFile.createReader(dataPosition, rc)) {
         if (in.isEOF()) {
            return null;
         }
         key = this.decorateKey(ByteBufferUtil.readWithShortLength(in));
      }
      return key;
   }

   public RowIndexEntry getExactPosition(final DecoratedKey dk, final SSTableReadsListener listener, final Rebufferer.ReaderConstraint rc) {
      if (!this.bf.isPresent(dk)) {
         listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.BLOOM_FILTER);
         Tracing.trace("Bloom filter allows skipping sstable {}", (Object)this.descriptor.generation);
         return null;
      }
      if ((this.filterFirst() && this.first.compareTo(dk) > 0) || (this.filterLast() && this.last.compareTo(dk) < 0)) {
         this.bloomFilterTracker.addFalsePositive();
         listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.MIN_MAX_KEYS);
         return null;
      }
      try (final PartitionIndex.Reader reader = this.partitionIndex.openReader(rc)) {
         final long indexPos = reader.exactCandidate(dk);
         if (indexPos == Long.MIN_VALUE) {
            this.bloomFilterTracker.addFalsePositive();
            listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.PARTITION_INDEX_LOOKUP);
            return null;
         }
         try (final FileDataInput in = this.createIndexOrDataReader(indexPos, rc)) {
            if (!ByteBufferUtil.equalsWithShortLength(in, dk.getKey())) {
               this.bloomFilterTracker.addFalsePositive();
               listener.onSSTableSkipped(this, SSTableReadsListener.SkippingReason.INDEX_ENTRY_NOT_FOUND);
               return null;
            }
            this.bloomFilterTracker.addTruePositive();
            final RowIndexEntry entry = (indexPos >= 0L) ? TrieIndexEntry.deserialize(in, in.getFilePointer()) : new RowIndexEntry(~indexPos);
            listener.onSSTableSelected(this, entry, SSTableReadsListener.SelectionReason.INDEX_ENTRY_FOUND);
            return entry;
         }
      }
      catch (IOException e) {
         this.markSuspect();
         throw new CorruptSSTableException(e, this.rowIndexFile.path());
      }
   }

   protected FileHandle[] getFilesToBeLocked() {
      return new FileHandle[]{this.dataFile, this.rowIndexFile, this.partitionIndex.getFileHandle()};
   }

   public PartitionIterator coveredKeysIterator(PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) throws IOException {
      return new PartitionIterator(this.partitionIndex, this.metadata().partitioner, this.rowIndexFile, this.dataFile, left, inclusiveLeft?-1:0, right, inclusiveRight?0:-1, Rebufferer.ReaderConstraint.NONE);
   }

   public PartitionIterator allKeysIterator() throws IOException {
      return new PartitionIterator(this.partitionIndex, this.metadata().partitioner, this.rowIndexFile, this.dataFile, Rebufferer.ReaderConstraint.NONE);
   }

   public ScrubPartitionIterator scrubPartitionsIterator() throws IOException {
      return this.partitionIndex == null?null:new ScrubIterator(this.partitionIndex, this.rowIndexFile);
   }

   public Flow<IndexFileEntry> coveredKeysFlow(RandomAccessReader dataFileReader, PartitionPosition left, boolean inclusiveLeft, PartitionPosition right, boolean inclusiveRight) {
      return new TrieIndexFileFlow(dataFileReader, this, left, inclusiveLeft?-1:0, right, inclusiveRight?0:-1);
   }

   public Iterable<DecoratedKey> getKeySamples(final Range<Token> range) {
      final Iterator<PartitionIndex.IndexPosIterator> partitionKeyIterators = SSTableScanner.makeBounds(this, Collections.singleton(range)).stream().map((bound) -> {
         return this.indexPosIteratorForRange(bound);
      }).iterator();
      return (Iterable)(!partitionKeyIterators.hasNext()?UnmodifiableArrayList.emptyList():new Iterable<DecoratedKey>() {
         public Iterator<DecoratedKey> iterator() {
            return new AbstractIterator<DecoratedKey>() {
               PartitionIndex.IndexPosIterator currentItr = (PartitionIndex.IndexPosIterator)partitionKeyIterators.next();
               long count = -1L;

               private long getNextPos() throws IOException {
                  long pos;
                  for(pos = -9223372036854775808L; (pos = this.currentItr.nextIndexPos()) == -9223372036854775808L && partitionKeyIterators.hasNext(); this.currentItr = (PartitionIndex.IndexPosIterator)partitionKeyIterators.next()) {
                     ;
                  }

                  return pos;
               }

               protected DecoratedKey computeNext() {
                  try {
                     while(true) {
                        long pos = this.getNextPos();
                        ++this.count;
                        if(pos == -9223372036854775808L) {
                           return (DecoratedKey)this.endOfData();
                        }

                        if(this.count % 128L == 0L) {
                           DecoratedKey key = TrieIndexSSTableReader.this.getKeyByPos(pos);
                           if(range.contains(key.getToken())) {
                              return key;
                           }

                           --this.count;
                        }
                     }
                  } catch (IOException var4) {
                     TrieIndexSSTableReader.this.markSuspect();
                     throw new CorruptSSTableException(var4, TrieIndexSSTableReader.this.dataFile.path());
                  }
               }
            };
         }
      });
   }

   private DecoratedKey getKeyByPos(long pos) throws IOException {
      assert pos != -9223372036854775808L;

      FileDataInput in;
      Throwable var4;
      DecoratedKey var5;
      if(pos >= 0L) {
         in = this.rowIndexFile.createReader(pos, Rebufferer.ReaderConstraint.NONE);
         var4 = null;

         try {
            var5 = this.metadata().partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
         } catch (Throwable var28) {
            var4 = var28;
            throw var28;
         } finally {
            if(in != null) {
               if(var4 != null) {
                  try {
                     in.close();
                  } catch (Throwable var27) {
                     var4.addSuppressed(var27);
                  }
               } else {
                  in.close();
               }
            }

         }

         return var5;
      } else {
         in = this.dataFile.createReader(~pos, Rebufferer.ReaderConstraint.NONE);
         var4 = null;

         try {
            var5 = this.metadata().partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
         } catch (Throwable var29) {
            var4 = var29;
            throw var29;
         } finally {
            if(in != null) {
               if(var4 != null) {
                  try {
                     in.close();
                  } catch (Throwable var26) {
                     var4.addSuppressed(var26);
                  }
               } else {
                  in.close();
               }
            }

         }

         return var5;
      }
   }

   private PartitionIndex.IndexPosIterator indexPosIteratorForRange(AbstractBounds<PartitionPosition> bound) {
      return new PartitionIndex.IndexPosIterator(this.partitionIndex, (PartitionPosition)bound.left, (PartitionPosition)bound.right, Rebufferer.ReaderConstraint.NONE);
   }

   public long estimatedKeysForRanges(final Collection<Range<Token>> ranges) {
      long estimatedKeyCounts = 0L;
      for (final AbstractBounds<PartitionPosition> bound : SSTableScanner.makeBounds(this, ranges)) {
         try (final PartitionIndex.IndexPosIterator iterator = this.indexPosIteratorForRange(bound)) {
            long pos = Long.MIN_VALUE;
            while ((pos = iterator.nextIndexPos()) != Long.MIN_VALUE) {
               ++estimatedKeyCounts;
            }
         }
         catch (IOException e) {
            this.markSuspect();
            throw new CorruptSSTableException(e, this.dataFile.path());
         }
      }
      return estimatedKeyCounts;
   }
}
