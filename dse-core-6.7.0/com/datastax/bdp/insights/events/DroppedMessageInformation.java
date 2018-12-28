package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.net.DroppedMessages.DroppedMessageGroupStats;
import org.apache.cassandra.net.DroppedMessages.Group;
import org.apache.cassandra.utils.time.ApproximateTime;

public class DroppedMessageInformation extends Insight {
   public static final String NAME = "dse.insights.event.dropped_messages";

   public DroppedMessageInformation(List<DroppedMessageGroupStats> stats) {
      super(new InsightMetadata("dse.insights.event.dropped_messages", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.empty()), new DroppedMessageInformation.Data(stats));
   }

   public static class DroppedMessageGroupInsight {
      @JsonProperty("group_name")
      public final Group group;
      @JsonProperty("reporting_interval_seconds")
      public final int reportingIntervalSeconds;
      @JsonProperty("num_internal")
      public final int internalDropped;
      @JsonProperty("num_cross_node")
      public final int crossNodeDropped;
      @JsonProperty("internal_latency_ms")
      public final long internalLatencyMs;
      @JsonProperty("crossnode_latency_ms")
      public final long crossNodeLatencyMs;

      public DroppedMessageGroupInsight(DroppedMessageGroupStats stat) {
         this.group = stat.group;
         this.reportingIntervalSeconds = stat.reportingIntervalSeconds;
         this.internalDropped = stat.internalDropped;
         this.crossNodeDropped = stat.crossNodeDropped;
         this.internalLatencyMs = stat.internalLatencyMs;
         this.crossNodeLatencyMs = stat.crossNodeLatencyMs;
      }
   }

   public static class Data {
      @JsonProperty("group_stats")
      public final List<DroppedMessageInformation.DroppedMessageGroupInsight> droppedMessageGroupStats;

      private Data(List<DroppedMessageGroupStats> stats) {
         this.droppedMessageGroupStats = (List)stats.stream().map(DroppedMessageInformation.DroppedMessageGroupInsight::<init>).collect(Collectors.toList());
      }
   }
}
