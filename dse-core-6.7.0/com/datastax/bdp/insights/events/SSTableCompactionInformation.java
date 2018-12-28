package com.datastax.bdp.insights.events;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SSTableCompactionInformation {
   @JsonProperty("filename")
   public final String filename;
   @JsonProperty("level")
   public final int level;
   @JsonProperty("total_rows")
   public final long totalRows;
   @JsonProperty("generation")
   public final int generation;
   @JsonProperty("version")
   public final String version;
   @JsonProperty("size_bytes")
   public final long sizeBytes;
   @JsonProperty("strategy")
   public final String strategy;

   public SSTableCompactionInformation(@JsonProperty("filename") String filename, @JsonProperty("level") int level, @JsonProperty("total_rows") long totalRows, @JsonProperty("generation") int generation, @JsonProperty("version") String version, @JsonProperty("size_bytes") long sizeBytes, @JsonProperty("strategy") String strategy) {
      this.filename = filename;
      this.level = level;
      this.totalRows = totalRows;
      this.generation = generation;
      this.version = version;
      this.sizeBytes = sizeBytes;
      this.strategy = strategy;
   }
}
