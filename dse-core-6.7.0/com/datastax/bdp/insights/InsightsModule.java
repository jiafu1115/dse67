package com.datastax.bdp.insights;

import com.codahale.metrics.Metric;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.datastax.bdp.insights.reporting.CompactionInformationReporter;
import com.datastax.bdp.insights.reporting.CompactionRuntimeManager;
import com.datastax.bdp.insights.reporting.DroppedMessageRuntimeManager;
import com.datastax.bdp.insights.reporting.ExceptionRuntimeManager;
import com.datastax.bdp.insights.reporting.FlushRuntimeManager;
import com.datastax.bdp.insights.reporting.GCInformationReporter;
import com.datastax.bdp.insights.reporting.GCInformationRuntimeManager;
import com.datastax.bdp.insights.reporting.GossipRuntimeManager;
import com.datastax.bdp.insights.reporting.NodeLocalInsightsReporter;
import com.datastax.bdp.insights.reporting.NodeLocalInsightsRuntimeManager;
import com.datastax.bdp.insights.reporting.SchemaChangeRuntimeManager;
import com.datastax.bdp.insights.rpc.InsightsRpc;
import com.datastax.bdp.insights.storage.config.DseLocalInsightsRuntimeConfigAccess;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfigAccess;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfigManager;
import com.datastax.bdp.insights.storage.credentials.DseInsightsTokenStore;
import com.datastax.bdp.insights.storage.schema.InsightsSchemaManager;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InsightsModule extends AbstractModule {
   private static final Logger logger = LoggerFactory.getLogger(InsightsModule.class);

   public InsightsModule() {
   }

   protected void configure() {
      this.bind(DseInsightsTokenStore.class);
      this.bind(InsightsPlugin.class);
      this.bind(InsightsClientRuntimeManager.class);
      this.bind(NodeLocalInsightsReporter.class);
      this.bind(NodeLocalInsightsRuntimeManager.class);
      this.bind(GCInformationReporter.class);
      this.bind(GCInformationRuntimeManager.class);
      this.bind(FlushRuntimeManager.class);
      this.bind(ExceptionRuntimeManager.class);
      this.bind(SchemaChangeRuntimeManager.class);
      this.bind(CompactionInformationReporter.class);
      this.bind(CompactionRuntimeManager.class);
      this.bind(GossipRuntimeManager.class);
      this.bind(DroppedMessageRuntimeManager.class);
      this.bind(InsightsRpc.class);
   }

   @Provides
   @Singleton
   InsightsRuntimeConfigAccess configAccess() {
      return new DseLocalInsightsRuntimeConfigAccess();
   }

   @Provides
   @Singleton
   DseHostIdProvider dseHostIdProvider() {
      return SystemInfo::getHostId;
   }

   @Provides
   @Singleton
   InsightsSchemaManager insightsSchemaManager() {
      return new InsightsSchemaManager();
   }

   @Provides
   @Singleton
   InsightsRuntimeConfigManager runtimeConfigManager(InsightsSchemaManager schemaManager, InsightsRuntimeConfigAccess access, ThreadPoolPlugin threadPoolPlugin, DseInsightsTokenStore tokenStore) {
      schemaManager.setupSchema();
      return new InsightsRuntimeConfigManager(access, threadPoolPlugin, tokenStore);
   }

   @Provides
   @Singleton
   InsightsClient insightsClient(DseInsightsTokenStore tokenStore, DseHostIdProvider dseHostIdProvider, InsightsRuntimeConfigManager runtimeConfigManager) {
      UUID hostId = dseHostIdProvider.getHostId();
      if(hostId == null) {
         throw new IllegalStateException("Cannot instantiate insights client with null host id");
      } else {
         Map<String, Metric> metrics = new HashMap();
         metrics.put("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
         metrics.put("jvm.gc", new GarbageCollectorMetricSet());
         metrics.put("jvm.memory", new MemoryUsageGaugeSet());
         metrics.put("jvm.fd.usage", new FileDescriptorRatioGauge());
         Iterator var6 = metrics.entrySet().iterator();

         while(var6.hasNext()) {
            Entry entry = (Entry)var6.next();

            try {
               CassandraMetricsRegistry.Metrics.register((String)entry.getKey(), (Metric)entry.getValue());
            } catch (IllegalArgumentException var10) {
               logger.debug(var10.toString());
            }
         }

         try {
            InsightsRuntimeConfig runtimeConfig = runtimeConfigManager.getRuntimeConfig();
            InsightsUnixSocketClient client = new InsightsUnixSocketClient(CassandraMetricsRegistry.Metrics, runtimeConfig, tokenStore);
            runtimeConfigManager.addConfigChangeListener(client);
            return client;
         } catch (Exception var9) {
            logger.error("Unable to initialize insights client due to error", var9);
            throw new RuntimeException(var9);
         }
      }
   }
}
