package org.apache.cassandra.db.view;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Long.valueOf;

class ViewBuilder {
   private static final Logger logger = LoggerFactory.getLogger(ViewBuilder.class);
   private static final int NUM_TASKS = Runtime.getRuntime().availableProcessors() * 4;
   private final ColumnFamilyStore baseCfs;
   private final View view;
   private final String ksName;
   private final UUID localHostId = SystemKeyspace.getLocalHostId();
   private final Set<Range<Token>> builtRanges = Sets.newConcurrentHashSet();
   private final Map<Range<Token>, Pair<Token, Long>> pendingRanges = Maps.newConcurrentMap();
   private final Set<ViewBuilderTask> tasks = Sets.newConcurrentHashSet();
   private volatile long keysBuilt = 0L;
   private volatile boolean isStopped = false;
   private volatile Future<?> future = Futures.immediateFuture(null);

   ViewBuilder(ColumnFamilyStore baseCfs, View view) {
      this.baseCfs = baseCfs;
      this.view = view;
      this.ksName = baseCfs.metadata.keyspace;
   }

   public void start() {
      if(((Boolean)TPCUtils.blockingGet(SystemKeyspace.isViewBuilt(this.ksName, this.view.name))).booleanValue()) {
         logger.debug("View already marked built for {}.{}", this.ksName, this.view.name);
         if(!((Boolean)TPCUtils.blockingGet(SystemKeyspace.isViewStatusReplicated(this.ksName, this.view.name))).booleanValue()) {
            this.updateDistributed();
         }
      } else {
         SystemDistributedKeyspace.startViewBuild(this.ksName, this.view.name, this.localHostId);
         logger.debug("Starting build of view({}.{}). Flushing base table {}.{}", new Object[]{this.ksName, this.view.name, this.ksName, this.baseCfs.name});
         this.baseCfs.forceBlockingFlush();
         this.loadStatusAndBuild();
      }

   }

   private void loadStatusAndBuild() {
      this.loadStatus();
      this.build();
   }

   private void loadStatus() {
      this.builtRanges.clear();
      this.pendingRanges.clear();
      (TPCUtils.blockingGet(SystemKeyspace.getViewBuildStatus(this.ksName, this.view.name))).forEach((range, pair) -> {
         Token lastToken = pair.left;
         if(lastToken != null && lastToken.equals(range.right)) {
            this.builtRanges.add(range);
            this.keysBuilt += ((Long)pair.right).longValue();
         } else {
            this.pendingRanges.put(range, pair);
         }

      });
   }

   private synchronized void build() {
      if (this.isStopped) {
         logger.debug("Stopped build for view({}.{}) after covering {} keys", new Object[]{this.ksName, this.view.name, this.keysBuilt});
         return;
      }
      Set<Range<Token>> newRanges = StorageService.instance.getLocalRanges(this.ksName).stream().map(r -> r.subtractAll(this.builtRanges)).flatMap(Collection::stream).map(r -> r.subtractAll(this.pendingRanges.keySet())).flatMap(Collection::stream).collect(Collectors.toSet());
      if (newRanges.isEmpty() && this.pendingRanges.isEmpty()) {
         this.finish();
         return;
      }
      DatabaseDescriptor.getPartitioner().splitter().
              map(s -> s.split(newRanges, NUM_TASKS)).
              orElse(newRanges).
              forEach(r -> this.pendingRanges.put(r, Pair.<Token,Long>create(null, 0L)));
      List futures = this.pendingRanges.entrySet().stream().
              map(e -> new ViewBuilderTask(this.baseCfs, this.view, e.getKey(), e.getValue().left, e.getValue().right)).
              peek(this.tasks::add).
              map(CompactionManager.instance::submitViewBuilder).
              collect(Collectors.toList());
      ListenableFuture future = Futures.allAsList(futures);
      Futures.addCallback((ListenableFuture)future, (FutureCallback)new FutureCallback<List<Long>>(){

         public void onSuccess(List<Long> result) {
            ViewBuilder.this.keysBuilt = ViewBuilder.this.keysBuilt + result.stream().mapToLong(x -> x).sum();
            ViewBuilder.this.builtRanges.addAll(ViewBuilder.this.pendingRanges.keySet());
            ViewBuilder.this.pendingRanges.clear();
            ViewBuilder.this.build();
         }

         public void onFailure(Throwable t) {
            if (t instanceof CompactionInterruptedException) {
               ViewBuilder.this.internalStop(true);
               ViewBuilder.this.keysBuilt = ViewBuilder.this.tasks.stream().mapToLong(ViewBuilderTask::keysBuilt).sum();
               logger.info("Interrupted build for view({}.{}) after covering {} keys", new Object[]{ViewBuilder.this.ksName, ViewBuilder.this.view.name, ViewBuilder.this.keysBuilt});
            } else {
               ScheduledExecutors.nonPeriodicTasks.schedule(() -> ViewBuilder.this.loadStatusAndBuild(), 5L, TimeUnit.MINUTES);
               logger.warn("Materialized View failed to complete, sleeping 5 minutes before restarting", t);
            }
         }
      }, MoreExecutors.directExecutor());
      this.future = future;
   }

   private void finish() {
      logger.debug("Marking view({}.{}) as built after covering {} keys ", new Object[]{this.ksName, this.view.name, Long.valueOf(this.keysBuilt)});
      SystemKeyspace.finishViewBuildStatus(this.ksName, this.view.name);
      this.updateDistributed();
   }

   private void updateDistributed() {
      try {
         SystemDistributedKeyspace.successfulViewBuild(this.ksName, this.view.name, this.localHostId);
         SystemKeyspace.setViewBuiltReplicated(this.ksName, this.view.name);
      } catch (Exception var2) {
         ScheduledExecutors.nonPeriodicTasks.schedule(this::updateDistributed, 5L, TimeUnit.MINUTES);
         logger.warn("Failed to update the distributed status of view, sleeping 5 minutes before retrying", var2);
      }

   }

   synchronized void stop() {
      boolean wasStopped = this.isStopped;
      this.internalStop(false);
      if(!wasStopped) {
         FBUtilities.waitOnFuture(this.future);
      }

   }

   private void internalStop(boolean isCompactionInterrupted) {
      this.isStopped = true;
      this.tasks.forEach((task) -> {
         task.stop(isCompactionInterrupted);
      });
   }
}
