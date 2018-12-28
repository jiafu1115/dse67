package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.google.inject.Inject;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionRuntimeManager implements InsightsRuntimeConfigComponent {
   private static final Logger logger = LoggerFactory.getLogger(CompactionRuntimeManager.class);
   private final CompactionInformationReporter reporter;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public CompactionRuntimeManager(CompactionInformationReporter reporter) {
      this.reporter = reporter;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         logger.debug("Configuration changed to enable insights, starting compaction information reporting");
         this.reporter.startReportingCompactionInformation();
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         logger.debug("Configuration changed to disable insights, stopping compaction information reporting");
         this.reporter.stopReportingCompactionInformation();
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.compaction_started");
   }
}
