package org.apache.cassandra.dht;

import java.math.BigDecimal;
import java.math.MathContext;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ReadCallback;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeEstimates {
   private static final Logger logger = LoggerFactory.getLogger(SizeEstimates.class);
   private final Map<String, SizeEstimates.KeyspaceSizeEstimates> keyspaceEstimates = new HashMap();
   private final IPartitioner partitioner;

   SizeEstimates(IPartitioner partitioner) {
      this.partitioner = partitioner;
   }

   public long getEstimatedPartitions(String keyspace, String table, Collection<InetAddress> peers, Range<Token> range) {
      SizeEstimates.KeyspaceSizeEstimates estimates = (SizeEstimates.KeyspaceSizeEstimates)this.keyspaceEstimates.computeIfAbsent(keyspace, (ks) -> {
         return new SizeEstimates.KeyspaceSizeEstimates(ks, this.partitioner);
      });
      return estimates.getEstimatedPartitions(table, peers, range);
   }

   public void clear() {
      this.keyspaceEstimates.clear();
   }

   static class SizeEstimatesHelper {
      SizeEstimatesHelper() {
      }

      static void fetchSizeEstimateFromPeer(InetAddress peer, SizeEstimates.TableSizeEstimates estimates) throws Exception {
         Keyspace keyspace = Keyspace.open(estimates.keyspace);
         long queryStartNanoTime = System.nanoTime();
         ReadCommand cmd = createSizeEstimatesReadCommand(keyspace.getName(), estimates.table);
         ReadCallback<FlowablePartition> handler = ReadCallback.forInitialRead(cmd, Collections.singletonList(peer), ReadContext.builder(cmd, ConsistencyLevel.ONE).build(queryStartNanoTime));
         MessagingService.instance().send((Request.Dispatcher)cmd.dispatcherTo(Collections.singletonList(peer)), handler);
         PartitionIterator iterator = FlowablePartitions.toPartitionsFiltered(handler.result());
         Throwable var8 = null;

         try {
            processSizeEstimatesResponse(estimates, cmd.metadata(), iterator);
         } catch (Throwable var17) {
            var8 = var17;
            throw var17;
         } finally {
            if(iterator != null) {
               if(var8 != null) {
                  try {
                     iterator.close();
                  } catch (Throwable var16) {
                     var8.addSuppressed(var16);
                  }
               } else {
                  iterator.close();
               }
            }

         }

      }

      private static void processSizeEstimatesResponse(SizeEstimates.TableSizeEstimates estimates, TableMetadata metadata, PartitionIterator iterator) {
         Token.TokenFactory tf = metadata.partitioner.getTokenFactory();
         ColumnMetadata partitionCountColumn = metadata.getColumn(ColumnIdentifier.getInterned("partitions_count", true));

         while(iterator.hasNext()) {
            RowIterator rows = (RowIterator)iterator.next();
            Throwable var6 = null;

            try {
               while(rows.hasNext()) {
                  Row row = (Row)rows.next();
                  Cell cell = row.getCell(partitionCountColumn);
                  ByteBuffer[] clusterings = row.clustering().getRawValues();
                  String rangeStart = (String)UTF8Type.instance.compose(clusterings[1]);
                  String rangeEnd = (String)UTF8Type.instance.compose(clusterings[2]);
                  long partitions = cell != null && cell.value().remaining() != 0?((Long)LongType.instance.compose(cell.value())).longValue():0L;
                  estimates.put(tf.fromString(rangeStart), tf.fromString(rangeEnd), partitions);
               }
            } catch (Throwable var21) {
               var6 = var21;
               throw var21;
            } finally {
               if(rows != null) {
                  if(var6 != null) {
                     try {
                        rows.close();
                     } catch (Throwable var20) {
                        var6.addSuppressed(var20);
                     }
                  } else {
                     rows.close();
                  }
               }

            }
         }

      }

      private static SinglePartitionReadCommand createSizeEstimatesReadCommand(String keyspace, String table) {
         TableMetadata sizeEstimates = Keyspace.open("system").getColumnFamilyStore("size_estimates").metadata();

         assert sizeEstimates.partitionKeyType instanceof UTF8Type;

         UTF8Type keyType = (UTF8Type)sizeEstimates.partitionKeyType;
         DecoratedKey key = sizeEstimates.partitioner.decorateKey(keyType.decompose(keyspace));
         Slices slice = Slices.with(sizeEstimates.comparator, Slice.make(sizeEstimates.comparator, new Object[]{table}));
         ClusteringIndexFilter filter = new ClusteringIndexSliceFilter(slice, false);
         return SinglePartitionReadCommand.create(sizeEstimates, ApolloTime.systemClockSecondsAsInt(), key, ColumnFilter.all(sizeEstimates), filter);
      }
   }

   public static class TableSizeEstimates {
      private final String keyspace;
      private final String table;
      private final Splitter splitter;
      private final Set<InetAddress> fetchedPeers = new HashSet();
      private final Map<Pair<Token, Token>, Long> rangeCount = new HashMap();

      TableSizeEstimates(String keyspace, String table, IPartitioner partitioner) {
         this.keyspace = keyspace;
         this.table = table;
         this.splitter = (Splitter)partitioner.splitter().orElse(null);
      }

      public long getEstimatedPartitions(InetAddress peer, Range<Token> range) {
         this.mayBeFetchSizeEstimate(peer);
         long partitions = 0L;

         Range unwrap;
         for(Iterator var5 = range.unwrap().iterator(); var5.hasNext(); partitions += ((Long)this.rangeCount.getOrDefault(Pair.create(unwrap.left, unwrap.right), Long.valueOf(0L))).longValue()) {
            unwrap = (Range)var5.next();
         }

         return partitions;
      }

      public void put(Token rangeStart, Token rangeEnd, long partitions) {
         Pair<Token, Token> key = Pair.create(rangeStart, rangeEnd);
         long current = ((Long)this.rangeCount.getOrDefault(key, Long.valueOf(0L))).longValue();
         this.rangeCount.put(key, Long.valueOf(partitions > current?partitions:current));
      }

      private void mayBeFetchSizeEstimate(InetAddress peer) {
         if(!this.fetchedPeers.contains(peer)) {
            try {
               SizeEstimates.SizeEstimatesHelper.fetchSizeEstimateFromPeer(peer, this);
            } catch (Exception var3) {
               SizeEstimates.logger.error("Unable to fetch size_estimates from peer {} for {}.{} (reason: {})", new Object[]{peer, this.keyspace, this.table, var3.getMessage()});
            }

            this.fetchedPeers.add(peer);
         }
      }

      long getProportionalEstimation(Range<Token> range) {
         if(this.splitter == null) {
            return (long)this.rangeCount.values().stream().mapToLong((v) -> {
               return v.longValue();
            }).average().orElse(0.0D);
         } else {
            double currentLength = ((Double)range.unwrap().stream().map((r) -> {
               return Double.valueOf(((Token)r.left).size((Token)r.right));
            }).reduce(Double.valueOf(0.0D), (a, b) -> {
               return Double.valueOf(a.doubleValue() + b.doubleValue());
            })).doubleValue();
            BigDecimal totalEstimation = BigDecimal.ZERO;
            double totalRingSize = 0.0D;

            Entry entry;
            for(Iterator var7 = this.rangeCount.entrySet().iterator(); var7.hasNext(); totalEstimation = totalEstimation.add(BigDecimal.valueOf(((Long)entry.getValue()).longValue()))) {
               entry = (Entry)var7.next();
               totalRingSize += ((Token)((Pair)entry.getKey()).left).size((Token)((Pair)entry.getKey()).right);
            }

            if(totalRingSize == 0.0D) {
               return 0L;
            } else {
               BigDecimal result = totalEstimation.multiply(BigDecimal.valueOf(currentLength / totalRingSize));

               try {
                  return result.round(MathContext.DECIMAL32).longValueExact();
               } catch (ArithmeticException var9) {
                  return 9223372036854775807L;
               }
            }
         }
      }
   }

   private static class KeyspaceSizeEstimates {
      private final String keyspace;
      private final IPartitioner partitioner;
      private final Map<String, SizeEstimates.TableSizeEstimates> tables = new HashMap();
      private final Map<InetAddress, Collection<Range<Token>>> primaryRangeCache = new HashMap();

      KeyspaceSizeEstimates(String keyspace, IPartitioner partitioner) {
         this.keyspace = keyspace;
         this.partitioner = partitioner;
      }

      private boolean isPrimaryRange(InetAddress peer, Range<Token> range) {
         Collection<Range<Token>> ranges = (Collection)this.primaryRangeCache.computeIfAbsent(peer, (p) -> {
            return StorageService.instance.getPrimaryRangesForEndpoint(this.keyspace, p);
         });
         return ranges != null && ranges.contains(range);
      }

      private long getEstimatedPartitions(String table, Collection<InetAddress> peers, Range<Token> range) {
         SizeEstimates.TableSizeEstimates estimates = (SizeEstimates.TableSizeEstimates)this.tables.computeIfAbsent(table, (t) -> {
            return new SizeEstimates.TableSizeEstimates(this.keyspace, t, this.partitioner);
         });
         peers.forEach((x$0) -> {
            estimates.mayBeFetchSizeEstimate(x$0);
         });
         Optional<InetAddress> primaryPeer = peers.stream().filter((h) -> {
            return this.isPrimaryRange(h, range);
         }).findFirst();
         return ((Long)primaryPeer.map((peer) -> {
            return Long.valueOf(estimates.getEstimatedPartitions(peer, range));
         }).orElseGet(() -> {
            return Long.valueOf(estimates.getProportionalEstimation(range));
         })).longValue();
      }
   }
}
