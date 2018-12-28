package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.datastax.bdp.insights.events.FlushInformation;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IFlushSubscriber;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ColumnFamilyStore.FlushReason;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FlushRuntimeManager implements InsightsRuntimeConfigComponent, IFlushSubscriber {
   private static final Logger logger = LoggerFactory.getLogger(FlushRuntimeManager.class);
   private final InsightsClient client;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public FlushRuntimeManager(InsightsClient client) {
      this.client = client;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         ColumnFamilyStore.registerFlushSubscriber(this);
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         ColumnFamilyStore.unregisterFlushSubscriber(this);
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.flush");
   }

   public void onFlush(TableMetadata tableMetadata, boolean isTruncate, FlushReason reason, Memtable memtable, List<SSTableReader> sstables, long durationInMillis) {
      try {
         this.client.report(new FlushInformation(tableMetadata, isTruncate, reason, memtable, sstables, durationInMillis));
      } catch (Exception var9) {
         logger.warn("Error reporting flush information to insights", var9);
      }

   }
}
