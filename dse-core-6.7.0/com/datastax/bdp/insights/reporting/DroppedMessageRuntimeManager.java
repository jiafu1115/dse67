package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.datastax.bdp.insights.events.DroppedMessageInformation;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.IDroppedMessageSubscriber;
import org.apache.cassandra.net.DroppedMessages.DroppedMessageGroupStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroppedMessageRuntimeManager implements InsightsRuntimeConfigComponent, IDroppedMessageSubscriber {
   private static final Logger logger = LoggerFactory.getLogger(DroppedMessageRuntimeManager.class);
   private final InsightsClient client;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public DroppedMessageRuntimeManager(InsightsClient client) {
      this.client = client;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         DroppedMessages.registerSubscriber(this);
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         DroppedMessages.unregisterSubscriber(this);
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.dropped_messages");
   }

   public void onMessageDropped(List<DroppedMessageGroupStats> stats) {
      try {
         this.client.report(new DroppedMessageInformation(stats));
      } catch (Exception var3) {
         logger.warn("Error reporting exception information to insights", var3);
      }

   }
}
