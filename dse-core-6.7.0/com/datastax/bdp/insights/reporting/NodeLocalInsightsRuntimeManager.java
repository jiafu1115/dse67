package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NodeLocalInsightsRuntimeManager implements InsightsRuntimeConfigComponent {
   private static final Logger logger = LoggerFactory.getLogger(NodeLocalInsightsRuntimeManager.class);
   private final NodeLocalInsightsReporter reporter;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public NodeLocalInsightsRuntimeManager(NodeLocalInsightsReporter reporter) {
      this.reporter = reporter;
   }

   public boolean shouldRestart(InsightsRuntimeConfig previousConfig, InsightsRuntimeConfig newConfig) {
      return newConfig.isEnabled() && !Objects.equals(previousConfig.config.node_system_info_report_period, newConfig.config.node_system_info_report_period);
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         logger.debug("Starting node system information reporting");
         this.reporter.startReportingNodeSystemInformation();
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         logger.debug("Configuration changed to disable insights, stopping node system information reporting");
         this.reporter.stopReportingNodeSystemInformation();
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.node_system_information");
   }
}
