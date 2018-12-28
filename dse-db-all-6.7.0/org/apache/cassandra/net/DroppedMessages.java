package org.apache.cassandra.net;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.StatusLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroppedMessages {
   private static final Logger logger = LoggerFactory.getLogger(DroppedMessages.class);
   private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;
   public static final CopyOnWriteArraySet<IDroppedMessageSubscriber> droppedMessageSubscribers = new CopyOnWriteArraySet();
   private final EnumMap<DroppedMessages.Group, DroppedMessageMetrics> metrics = new EnumMap(DroppedMessages.Group.class);

   DroppedMessages() {
      DroppedMessages.Group[] var1 = DroppedMessages.Group.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         DroppedMessages.Group group = var1[var3];
         this.metrics.put(group, new DroppedMessageMetrics(group));
      }

   }

   void scheduleLogging() {
      ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::log, 5000L, 5000L, TimeUnit.MILLISECONDS);
   }

   void onDroppedMessage(Message<?> message) {
      DroppedMessageMetrics messageMetrics = (DroppedMessageMetrics)this.metrics.get(message.verb().droppedGroup());
      if(messageMetrics == null) {
         NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 5L, TimeUnit.MINUTES, "Cannot increment dropped message for message {}", new Object[]{message});
      } else {
         messageMetrics.onMessageDropped(message.lifetimeMillis(), !message.isLocal());
      }

   }

   private void log() {
      List<DroppedMessages.DroppedMessageGroupStats> stats = this.getDroppedMessagesStats();
      Iterator var2 = stats.iterator();

      while(var2.hasNext()) {
         DroppedMessages.DroppedMessageGroupStats stat = (DroppedMessages.DroppedMessageGroupStats)var2.next();
         logger.info("{} messages were dropped in last {} ms: {} internal and {} cross node. Mean internal dropped latency: {} ms and Mean cross-node dropped latency: {} ms", new Object[]{stat.group, Integer.valueOf(stat.reportingIntervalSeconds), Integer.valueOf(stat.internalDropped), Integer.valueOf(stat.crossNodeDropped), Long.valueOf(stat.internalLatencyMs), Long.valueOf(stat.crossNodeLatencyMs)});
      }

      if(stats.size() > 0) {
         var2 = droppedMessageSubscribers.iterator();

         while(var2.hasNext()) {
            IDroppedMessageSubscriber subscriber = (IDroppedMessageSubscriber)var2.next();

            try {
               subscriber.onMessageDropped(stats);
            } catch (Throwable var5) {
               JVMStabilityInspector.inspectThrowable(var5);
               logger.warn("Exception notifying dropped message subscriber", var5);
            }
         }

         StatusLogger.log();
      }

   }

   @VisibleForTesting
   List<DroppedMessages.DroppedMessageGroupStats> getDroppedMessagesStats() {
      List<DroppedMessages.DroppedMessageGroupStats> ret = new ArrayList();
      DroppedMessages.Group[] var2 = DroppedMessages.Group.values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         DroppedMessages.Group group = var2[var4];
         DroppedMessageMetrics groupMetrics = (DroppedMessageMetrics)this.metrics.get(group);
         int internalDropped = groupMetrics.getAndResetInternalDropped();
         int crossNodeDropped = groupMetrics.getAndResetCrossNodeDropped();
         if(internalDropped > 0 || crossNodeDropped > 0) {
            long internalLatency = TimeUnit.NANOSECONDS.toMillis((long)groupMetrics.internalDroppedLatency.getSnapshot().getMean());
            long crossNodeLatency = TimeUnit.NANOSECONDS.toMillis((long)groupMetrics.crossNodeDroppedLatency.getSnapshot().getMean());
            ret.add(new DroppedMessages.DroppedMessageGroupStats(group, internalDropped, crossNodeDropped, internalLatency, crossNodeLatency));
         }
      }

      return ret;
   }

   Map<String, Integer> getSnapshot() {
      Map<String, Integer> map = new HashMap(DroppedMessages.Group.values().length);
      DroppedMessages.Group[] var2 = DroppedMessages.Group.values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         DroppedMessages.Group group = var2[var4];
         map.put(group.toString(), Integer.valueOf((int)((DroppedMessageMetrics)this.metrics.get(group)).dropped.getCount()));
      }

      return map;
   }

   Map<DroppedMessages.Group, DroppedMessageMetrics> getAllMetrics() {
      return Collections.unmodifiableMap(this.metrics);
   }

   public static void registerSubscriber(IDroppedMessageSubscriber subscriber) {
      droppedMessageSubscribers.add(subscriber);
   }

   public static void unregisterSubscriber(IDroppedMessageSubscriber subscriber) {
      droppedMessageSubscribers.remove(subscriber);
   }

   public static class DroppedMessageGroupStats {
      public final DroppedMessages.Group group;
      public final int reportingIntervalSeconds;
      public final int internalDropped;
      public final int crossNodeDropped;
      public final long internalLatencyMs;
      public final long crossNodeLatencyMs;

      DroppedMessageGroupStats(DroppedMessages.Group group, int internalDropped, int crossNodeDropped, long internalLatencyMs, long crossNodeLatencyMs) {
         this.group = group;
         this.reportingIntervalSeconds = 5000;
         this.internalDropped = internalDropped;
         this.crossNodeDropped = crossNodeDropped;
         this.internalLatencyMs = internalLatencyMs;
         this.crossNodeLatencyMs = crossNodeLatencyMs;
      }
   }

   public static enum Group {
      MUTATION,
      COUNTER_MUTATION,
      VIEW_MUTATION,
      BATCH_STORE,
      READ,
      RANGE_SLICE,
      READ_REPAIR,
      LWT,
      HINT,
      TRUNCATE,
      SNAPSHOT,
      SCHEMA,
      REPAIR,
      NODESYNC,
      OTHER;

      private Group() {
      }
   }
}
