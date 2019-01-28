package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowPurger;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.time.ApolloTime;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReadCommand extends AbstractReadQuery implements SchedulableMessage {
   protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);
   @Nullable
   private final IndexMetadata index;
   @Nullable
   private final DigestVersion digestVersion;

   protected ReadCommand(DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, IndexMetadata index, TPCTaskType readType) {
      super(metadata, nowInSec, columnFilter, rowFilter, limits, readType);
      this.digestVersion = digestVersion;
      this.index = index;
   }

   protected abstract void serializeSelection(DataOutputPlus var1, ReadVerbs.ReadVersion var2) throws IOException;

   protected abstract long selectionSerializedSize(ReadVerbs.ReadVersion var1);

   public abstract boolean isLimitedToOnePartition();

   public abstract Request.Dispatcher<? extends ReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> var1);

   public abstract Request<? extends ReadCommand, ReadResponse> requestTo(InetAddress var1);

   public abstract ReadCommand withUpdatedLimit(DataLimits var1);

   public Supplier<StagedScheduler> getSchedulerSupplier() {
      return TPC::bestTPCScheduler;
   }

   public abstract long getTimeout();

   public boolean isDigestQuery() {
      return this.digestVersion != null;
   }

   @Nullable
   public DigestVersion digestVersion() {
      return this.digestVersion;
   }

   public abstract ReadCommand createDigestCommand(DigestVersion var1);

   @Nullable
   public IndexMetadata indexMetadata() {
      return this.index;
   }

   public abstract ClusteringIndexFilter clusteringIndexFilter(DecoratedKey var1);

   @VisibleForTesting
   public abstract Flow<FlowableUnfilteredPartition> queryStorage(ColumnFamilyStore var1, ReadExecutionController var2);

   protected abstract int oldestUnrepairedTombstone();

   public abstract boolean isReversed();

   Single<ReadResponse> createResponse(Flow<FlowableUnfilteredPartition> partitions, boolean forLocalDelivery) {
      return this.isDigestQuery()?ReadResponse.createDigestResponse(partitions, this):ReadResponse.createDataResponse(partitions, this, forLocalDelivery);
   }

   long indexSerializedSize() {
      return null != this.index?IndexMetadata.serializer.serializedSize(this.index):0L;
   }

   public Index getIndex() {
      return null != this.index?IndexRegistry.obtain(this.metadata()).getIndex(this.index):null;
   }

   public Index getIndex(ColumnFamilyStore cfs) {
      return null != this.index?cfs.indexManager.getIndex(this.index):null;
   }

   static IndexMetadata findIndex(TableMetadata table, RowFilter rowFilter) {
      if(!table.indexes.isEmpty() && !rowFilter.isEmpty()) {
         ColumnFamilyStore cfs = Keyspace.openAndGetStore(table);
         Index index = cfs.indexManager.getBestIndexFor(rowFilter);
         return null != index?index.getIndexMetadata():null;
      } else {
         return null;
      }
   }

   public void maybeValidateIndex() {
      if(null != this.index) {
         IndexRegistry.obtain(this.metadata()).getIndex(this.index).validate(this);
      }

   }

   public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor) {
      long startTimeNanos = System.nanoTime();
      ColumnFamilyStore cfs = Keyspace.openAndGetStore(this.metadata());
      Index index = this.getIndex(cfs);
      Index.Searcher pickSearcher = null;
      if(index != null) {
         if(!cfs.indexManager.isIndexQueryable(index)) {
            throw new IndexNotAvailableException(index);
         }

         pickSearcher = index.searcherFor(this);
         Tracing.trace("Executing read on {}.{} using index {}", new Object[]{cfs.metadata.keyspace, cfs.metadata.name, index.getIndexMetadata().name});
      }
      Index.Searcher searcher =pickSearcher;
      Flow<FlowableUnfilteredPartition> flow = this.applyController((controller) -> {
         Flow<FlowableUnfilteredPartition> r = searcher == null?this.queryStorage(cfs, controller):searcher.search(controller);
         if(monitor != null) {
            r = monitor.withMonitoring(r);
         }

         r = this.withoutPurgeableTombstones(r, cfs);
         r = this.withMetricsRecording(r, cfs.metric, startTimeNanos);
         RowFilter updatedFilter = searcher == null?this.rowFilter():index.getPostIndexQueryFilter(this.rowFilter());
         r = updatedFilter.filter(r, cfs.metadata(), this.nowInSec());
         return this.limits().truncateUnfiltered(r, this.nowInSec(), this.selectsFullPartition(), this.metadata().rowPurger());
      });
      return flow;
   }

   public Flow<FlowableUnfilteredPartition> applyController(Function<ReadExecutionController, Flow<FlowableUnfilteredPartition>> op) {
      return Flow.using(() -> {
         return ReadExecutionController.forCommand(this);
      }, op, ReadExecutionController::close);
   }

   protected abstract void recordLatency(TableMetrics var1, long var2);

   public ReadExecutionController executionController() {
      return ReadExecutionController.forCommand(this);
   }

   protected boolean shouldRespectTombstoneThresholds() {
      return !SchemaConstants.isLocalSystemKeyspace(this.metadata().keyspace);
   }

   private Flow<FlowableUnfilteredPartition> withMetricsRecording(Flow<FlowableUnfilteredPartition> partitions, final TableMetrics metric, final long startTimeNanos) {
      class MetricRecording {
         private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
         private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();
         private final boolean shouldRespectTombstoneThresholds = ReadCommand.this.shouldRespectTombstoneThresholds();
         private final RowPurger rowPurger = ReadCommand.this.metadata().rowPurger();
         private int liveRows = 0;
         private int tombstones = 0;
         private DecoratedKey currentKey;

         MetricRecording() {
         }

         public FlowableUnfilteredPartition countPartition(FlowableUnfilteredPartition iter) {
            this.currentKey = iter.header().partitionKey;
            this.countRow(iter.staticRow());
            return iter.mapContent(this::countUnfiltered);
         }

         public Unfiltered countUnfiltered(Unfiltered unfiltered) {
            if(unfiltered.isRow()) {
               this.countRow((Row)unfiltered);
            } else {
               this.countTombstone(unfiltered.clustering(), 1);
            }

            return unfiltered;
         }

         public void countRow(Row row) {
            Pair<Boolean, Integer> isAliveAndTombstones = this.rowPurger.isAliveRowAndCountTombstones(row, ReadCommand.this.nowInSec());
            if(((Boolean)isAliveAndTombstones.left).booleanValue()) {
               ++this.liveRows;
            }

            this.countTombstone(row.clustering(), ((Integer)isAliveAndTombstones.right).intValue());
         }

         private void countTombstone(ClusteringPrefix clustering, int count) {
            this.tombstones += count;
            if(this.tombstones > this.failureThreshold && this.shouldRespectTombstoneThresholds) {
               String query = ReadCommand.this.toCQLString();
               Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", Integer.valueOf(this.failureThreshold), query);
               metric.tombstoneFailures.inc();
               throw new TombstoneOverwhelmingException(this.tombstones, query, ReadCommand.this.metadata(), this.currentKey, clustering);
            }
         }

         public void onComplete() {
            ReadCommand.this.recordLatency(metric, ApolloTime.approximateNanoTime() - startTimeNanos);
            metric.tombstoneScannedHistogram.update(this.tombstones);
            metric.liveScannedHistogram.update(this.liveRows);
            boolean warnTombstones = this.tombstones > this.warningThreshold && this.shouldRespectTombstoneThresholds;
            if(warnTombstones) {
               String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", new Object[]{Integer.valueOf(this.liveRows), Integer.valueOf(this.tombstones), ReadCommand.this.toCQLString()});
               ClientWarn.instance.warn(msg);
               if(this.tombstones < this.failureThreshold) {
                  metric.tombstoneWarnings.inc();
               }

               ReadCommand.logger.warn(msg);
            }

            Tracing.trace("Read {} live rows and {} tombstone cells{}", new Object[]{Integer.valueOf(this.liveRows), Integer.valueOf(this.tombstones), warnTombstones?" (see tombstone_warn_threshold)":""});
         }
      }

      MetricRecording metricsRecording = new MetricRecording();
      metricsRecording.getClass();
      partitions = partitions.map(metricsRecording::countPartition);
      metricsRecording.getClass();
      partitions = partitions.doOnClose(metricsRecording::onComplete);
      return partitions;
   }

   protected Flow<FlowableUnfilteredPartition> withoutPurgeableTombstones(Flow<FlowableUnfilteredPartition> iterator, ColumnFamilyStore cfs) {
      ReadCommand.PurgeOp var10001 = new ReadCommand.PurgeOp(this.nowInSec(), cfs.gcBefore(this.nowInSec()), this::oldestUnrepairedTombstone, cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(), cfs.metadata().rowPurger());
      var10001.getClass();
      return iterator.map(var10001::purgePartition);
   }

   protected boolean isSame(Object other) {
      if(other != null && this.getClass().equals(other.getClass())) {
         ReadCommand that = (ReadCommand)other;
         return super.isSame(that) && Objects.equals(this.index, that.index) && Objects.equals(this.digestVersion, that.digestVersion);
      } else {
         return false;
      }
   }

   protected static class ReadCommandSerializer<T extends ReadCommand> extends VersionDependent<ReadVerbs.ReadVersion> implements Serializer<T> {
      private final ReadCommand.SelectionDeserializer<T> selectionDeserializer;
      private final ColumnFilter.Serializer columnFilterSerializer;
      private final RowFilter.Serializer rowFilterSerializer;
      private final DataLimits.Serializer dataLimitsSerializer;

      protected ReadCommandSerializer(ReadVerbs.ReadVersion version, ReadCommand.SelectionDeserializer<T> selectionDeserializer) {
         super(version);
         this.selectionDeserializer = selectionDeserializer;
         this.columnFilterSerializer = (ColumnFilter.Serializer)ColumnFilter.serializers.get(version);
         this.rowFilterSerializer = (RowFilter.Serializer)RowFilter.serializers.get(version);
         this.dataLimitsSerializer = (DataLimits.Serializer)DataLimits.serializers.get(version);
      }

      private static int digestFlag(boolean isDigest) {
         return isDigest?1:0;
      }

      private static boolean isDigest(int flags) {
         return (flags & 1) != 0;
      }

      private static boolean isForThrift(int flags) {
         return (flags & 2) != 0;
      }

      private static int indexFlag(boolean hasIndex) {
         return hasIndex?4:0;
      }

      private static boolean hasIndex(int flags) {
         return (flags & 4) != 0;
      }

      private int digestVersionInt(DigestVersion digestVersion) {
         return ((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) < 0?MessagingVersion.OSS_30.protocolVersion().handshakeVersion:digestVersion.ordinal();
      }

      private DigestVersion fromDigestVersionInt(int digestVersion) {
         if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) >= 0) {
            return DigestVersion.values()[digestVersion];
         } else {
            MessagingVersion ms = MessagingVersion.fromHandshakeVersion(digestVersion);
            return ((ReadVerbs.ReadVersion)ms.groupVersion(Verbs.Group.READS)).digestVersion;
         }
      }

      public void serialize(T command, DataOutputPlus out) throws IOException {
         if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) < 0) {
            out.writeByte(command instanceof SinglePartitionReadCommand?0:1);
         }

         out.writeByte(digestFlag(command.isDigestQuery()) | indexFlag(null != command.indexMetadata()));
         if(command.isDigestQuery()) {
            out.writeUnsignedVInt((long)this.digestVersionInt(command.digestVersion()));
         }

         command.metadata().id.serialize(out);
         out.writeInt(command.nowInSec());
         this.columnFilterSerializer.serialize(command.columnFilter(), out);
         this.rowFilterSerializer.serialize(command.rowFilter(), out);
         this.dataLimitsSerializer.serialize(command.limits(), out, command.metadata().comparator);
         if(null != command.indexMetadata()) {
            IndexMetadata.serializer.serialize(command.indexMetadata(), out);
         }

         command.serializeSelection(out, (ReadVerbs.ReadVersion)this.version);
      }

      public T deserialize(DataInputPlus in) throws IOException {
         if(((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) < 0) {
            in.readByte();
         }

         int flags = in.readByte();
         boolean isDigest = isDigest(flags);
         if(isForThrift(flags)) {
            throw new IllegalStateException("Received a command with the thrift flag set. This means thrift is in use in a mixed 3.0/3.X and 4.0+ cluster, which is unsupported. Make sure to stop using thrift before upgrading to 4.0");
         } else {
            boolean hasIndex = hasIndex(flags);
            DigestVersion digestVersion = isDigest?this.fromDigestVersionInt((int)in.readUnsignedVInt()):null;
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            int nowInSec = in.readInt();
            ColumnFilter columnFilter = this.columnFilterSerializer.deserialize(in, metadata);
            RowFilter rowFilter = this.rowFilterSerializer.deserialize(in, metadata);
            DataLimits limits = this.dataLimitsSerializer.deserialize(in, metadata);
            IndexMetadata index = hasIndex?this.deserializeIndexMetadata(in, metadata):null;
            return this.selectionDeserializer.deserialize(in, (ReadVerbs.ReadVersion)this.version, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
         }
      }

      private IndexMetadata deserializeIndexMetadata(DataInputPlus in, TableMetadata metadata) throws IOException {
         try {
            return IndexMetadata.serializer.deserialize(in, metadata);
         } catch (UnknownIndexException var4) {
            ReadCommand.logger.info("Couldn't find a defined index on {}.{} with the id {}. If an index was just created, this is likely due to the schema not being fully propagated. Local read will proceed without using the index. Please wait for schema agreement after index creation.", new Object[]{metadata.keyspace, metadata.name, var4.indexId});
            return null;
         }
      }

      public long serializedSize(T command) {
         return (long)(1 + (((ReadVerbs.ReadVersion)this.version).compareTo(ReadVerbs.ReadVersion.DSE_60) < 0?1:0) + (command.isDigestQuery()?TypeSizes.sizeofUnsignedVInt((long)this.digestVersionInt(command.digestVersion())):0) + command.metadata().id.serializedSize() + TypeSizes.sizeof(command.nowInSec())) + this.columnFilterSerializer.serializedSize(command.columnFilter()) + this.rowFilterSerializer.serializedSize(command.rowFilter()) + this.dataLimitsSerializer.serializedSize(command.limits(), command.metadata().comparator) + command.indexSerializedSize() + command.selectionSerializedSize((ReadVerbs.ReadVersion)this.version);
      }
   }

   static class PurgeOp {
      private final DeletionPurger purger;
      private final int nowInSec;
      private final RowPurger rowPurger;

      public PurgeOp(int nowInSec, int gcBefore, Supplier<Integer> oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones, RowPurger rowPurger) {
         this.nowInSec = nowInSec;
         this.purger = (timestamp, localDeletionTime) -> {
            return (!onlyPurgeRepairedTombstones || localDeletionTime < ((Integer)oldestUnrepairedTombstone.get()).intValue()) && localDeletionTime < gcBefore;
         };
         this.rowPurger = rowPurger;
      }

      public FlowableUnfilteredPartition purgePartition(final FlowableUnfilteredPartition partition) {
         final PartitionHeader header = partition.header();
         class Purged extends FlowableUnfilteredPartition.SkippingMap {
            Purged() {
               super(partition.content(), PurgeOp.this.applyToStatic(partition.staticRow()), PurgeOp.this.purger.shouldPurge(header.partitionLevelDeletion)?header.with(DeletionTime.LIVE):header, PurgeOp.this::purgeUnfiltered);
            }
         }

         return new Purged();
      }

      public Unfiltered purgeUnfiltered(Unfiltered next) {
         return next.purge(this.purger, this.nowInSec, this.rowPurger);
      }

      public Row applyToStatic(Row row) {
         Row purged = row.purge(this.purger, this.nowInSec, this.rowPurger);
         return purged != null?purged:Rows.EMPTY_STATIC_ROW;
      }
   }

   protected abstract static class SelectionDeserializer<T extends ReadCommand> {
      protected SelectionDeserializer() {
      }

      public abstract T deserialize(DataInputPlus var1, ReadVerbs.ReadVersion var2, DigestVersion var3, TableMetadata var4, int var5, ColumnFilter var6, RowFilter var7, DataLimits var8, IndexMetadata var9) throws IOException;
   }
}
