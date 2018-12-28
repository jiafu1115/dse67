package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.events.DSENodeConfiguration;
import com.datastax.bdp.insights.events.DSENodeSystemInformation;
import com.datastax.bdp.insights.events.DSESchemaInformation;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfigManager;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NodeLocalInsightsReporter {
   private static final Logger logger = LoggerFactory.getLogger(NodeLocalInsightsReporter.class);
   private final InsightsClient insightsClient;
   private ScheduledFuture<?> nodeSystemInfoReports;
   private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
   private final InsightsRuntimeConfigManager runtimeConfigManager;

   @Inject
   public NodeLocalInsightsReporter(InsightsClient insightsClient, InsightsRuntimeConfigManager runtimeConfigManager) {
      this.insightsClient = insightsClient;
      this.runtimeConfigManager = runtimeConfigManager;
   }

   public void startReportingNodeSystemInformation() {
      long nodeSystemInfoReportPeriod = Duration.parse(this.runtimeConfigManager.getRuntimeConfig().config.node_system_info_report_period).toMillis();
      this.nodeSystemInfoReports = this.executorService.scheduleAtFixedRate(this::reportSystemInformation, 60000L, nodeSystemInfoReportPeriod, TimeUnit.MILLISECONDS);
   }

   public void stopReportingNodeSystemInformation() {
      try {
         if(this.nodeSystemInfoReports != null) {
            this.nodeSystemInfoReports.cancel(true);
         }
      } catch (Exception var2) {
         logger.debug("Error canceling node system information reporting: ", var2);
      }

   }

   private void reportSystemInformation() {
      try {
         DSENodeSystemInformation dseNodeSystemInformation = new DSENodeSystemInformation();
         this.insightsClient.report(dseNodeSystemInformation);
      } catch (Exception var4) {
         logger.warn("Error reporting node system information", var4);
      }

      try {
         DSENodeConfiguration dseNodeConfiguration = new DSENodeConfiguration();
         this.insightsClient.report(dseNodeConfiguration);
      } catch (Exception var3) {
         logger.warn("Error reporting node configuration information", var3);
      }

      try {
         DSESchemaInformation dseSchemaInformation = new DSESchemaInformation();
         this.insightsClient.report(dseSchemaInformation);
      } catch (Exception var2) {
         logger.warn("Error reporting node schema information", var2);
      }

   }
}
