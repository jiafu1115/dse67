package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class GCInformationRuntimeManager implements InsightsRuntimeConfigComponent {
   private static final Logger logger = LoggerFactory.getLogger(GCInformationRuntimeManager.class);
   private final GCInformationReporter reporter;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public GCInformationRuntimeManager(GCInformationReporter reporter) {
      this.reporter = reporter;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         logger.debug("Configuration changed to enable insights, starting gc information reporting");
         this.reporter.startReportingGCInformation();
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         logger.debug("Configuration changed to disable insights, stopping gc information reporting");
         this.reporter.stopReportingGCInformation();
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.gc_information");
   }
}
