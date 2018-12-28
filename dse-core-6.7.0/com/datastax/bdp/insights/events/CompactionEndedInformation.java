package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.utils.time.ApolloTime;

@JsonIgnoreProperties(
   ignoreUnknown = true
)
public final class CompactionEndedInformation extends Insight {
   public static final String NAME = "dse.insights.event.compaction_ended";

   @JsonCreator
   public CompactionEndedInformation(@JsonProperty("metadata") InsightMetadata metadata, @JsonProperty("data") CompactionEndedInformation.Data data) {
      super(metadata, data);
   }

   @JsonCreator
   public CompactionEndedInformation(UUID id, String keyspace, String table, OperationType type, long compactedBytes, long totalBytes, boolean isStopRequested, long totalSourceCQLRows, long totalSSTableSizeBytes, List<SSTableCompactionInformation> sstables) {
      super(new InsightMetadata("dse.insights.event.compaction_ended", Long.valueOf(ApolloTime.systemClockMillis())), new CompactionEndedInformation.Data(id, keyspace, table, type, compactedBytes, totalBytes, isStopRequested, totalSourceCQLRows, totalSSTableSizeBytes, sstables));
   }

   public static class Data {
      @JsonProperty("id")
      public final UUID id;
      @JsonProperty("keyspace")
      public final String keyspace;
      @JsonProperty("table")
      public final String table;
      @JsonProperty("type")
      public final OperationType type;
      @JsonProperty("compacted_bytes")
      public final long compactedBytes;
      @JsonProperty("total_bytes")
      public final long totalBytes;
      @JsonProperty("is_stop_requested")
      public final boolean isStopRequested;
      @JsonProperty("total_source_cql_rows")
      public final long totalSourceCQLRows;
      @JsonProperty("total_sstable_size_bytes")
      public final long totalSSTableSizeBytes;
      @JsonProperty("sstables")
      public final List<SSTableCompactionInformation> sstables;

      public Data(@JsonProperty("id") UUID id, @JsonProperty("keyspace") String keyspace, @JsonProperty("table") String table, @JsonProperty("type") OperationType type, @JsonProperty("compacted_bytes") long compactedBytes, @JsonProperty("total_bytes") long totalBytes, @JsonProperty("is_stop_requested") boolean isStopRequested, @JsonProperty("total_source_cql_rows") long totalSourceCQLRows, @JsonProperty("sstable_size_bytes") long totalSSTableSizeBytes, @JsonProperty("sstables") List<SSTableCompactionInformation> sstables) {
         this.id = id;
         this.keyspace = keyspace;
         this.table = table;
         this.type = type;
         this.compactedBytes = compactedBytes;
         this.totalBytes = totalBytes;
         this.isStopRequested = isStopRequested;
         this.totalSourceCQLRows = totalSourceCQLRows;
         this.totalSSTableSizeBytes = totalSSTableSizeBytes;
         this.sstables = sstables;
      }
   }
}
