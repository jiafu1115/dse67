package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.cassandra.utils.time.ApolloTime;

@JsonIgnoreProperties(
   ignoreUnknown = true
)
public final class GCInformation extends Insight {
   public static final String NAME = "dse.insights.event.gc_information";

   @JsonCreator
   public GCInformation(@JsonProperty("metadata") InsightMetadata metadata, @JsonProperty("data") GCInformation.Data data) {
      super(metadata, data);
   }

   public GCInformation(String gcName, long duration, Map<String, Long> memoryUsageBefore, Map<String, Long> memoryUsageAfter, long promoted_bytes, long youngGcCpuNanos, long oldGcCpuNanos) {
      super(new InsightMetadata("dse.insights.event.gc_information", Long.valueOf(ApolloTime.systemClockMillis())), new GCInformation.Data(gcName, duration, memoryUsageBefore, memoryUsageAfter, promoted_bytes, youngGcCpuNanos, oldGcCpuNanos));
   }

   public static class Data {
      @JsonProperty("gc_name")
      public final String gcName;
      @JsonProperty("duration")
      public final long duration;
      @JsonProperty("memory_usage_before")
      public final Map<String, Long> memoryUsageBefore;
      @JsonProperty("memory_usage_after")
      public final Map<String, Long> memoryUsageAfter;
      @JsonProperty("promoted_bytes")
      public final long promotedBytes;
      @JsonProperty("young_gc_cpu_nanos")
      public final long youngGcCpuNanos;
      @JsonProperty("old_gc_cpu_nanos")
      public final long oldGcCpuNanos;

      public Data(@JsonProperty("gc_name") String gcName, @JsonProperty("duration") long duration, @JsonProperty("memory_usage_before") Map<String, Long> memoryUsageBefore, @JsonProperty("memory_usage_after") Map<String, Long> memoryUsageAfter, @JsonProperty("promoted_bytes") long promoted_bytes, @JsonProperty("young_gc_cpu_nanos") long youngGcCpuNanos, @JsonProperty("old_gc_cpu_nanos") long oldGcCpuNanos) {
         this.duration = duration;
         this.gcName = gcName;
         this.memoryUsageBefore = memoryUsageBefore;
         this.memoryUsageAfter = memoryUsageAfter;
         this.promotedBytes = promoted_bytes;
         this.youngGcCpuNanos = youngGcCpuNanos;
         this.oldGcCpuNanos = oldGcCpuNanos;
      }
   }
}
