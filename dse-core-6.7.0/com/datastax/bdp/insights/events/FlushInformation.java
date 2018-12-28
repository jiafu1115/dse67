package com.datastax.bdp.insights.events;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ColumnFamilyStore.FlushReason;
import org.apache.cassandra.db.Memtable.MemoryUsage;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.time.ApproximateTime;

public class FlushInformation extends Insight {
   public static final String NAME = "dse.insights.event.flush";
   private static final String MAPPING_VERSION = "dse-flush-" + ProductVersion.getDSEVersionString();

   public FlushInformation(TableMetadata tableMetadata, boolean isTruncate, FlushReason reason, Memtable memtable, List<SSTableReader> sstables, long durationInMillis) {
      super(new InsightMetadata("dse.insights.event.flush", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.of(MAPPING_VERSION)), new FlushInformation.Data(tableMetadata, isTruncate, reason, memtable, sstables, durationInMillis));
   }

   public static class Data {
      @JsonProperty("keyspace")
      public final String keyspace;
      @JsonProperty("table")
      public final String table;
      @JsonProperty("flush_reason")
      public final FlushReason reason;
      @JsonProperty("is_truncate")
      public final boolean truncate;
      @JsonProperty("memtable_memory_use")
      public final MemoryUsage memtableMemoryUsage;
      @JsonProperty("memtable_live_data_size")
      public final long memtableLiveDataSize;
      @JsonProperty("flush_duration_millis")
      public final long flushDurationMillis;
      @JsonProperty("sstables_flushed")
      public final int sstablesFlushed;
      @JsonProperty("sstable_disk_size")
      public final long sstableBytesOnDisk;
      @JsonProperty("partitions_flushed")
      public final long partitonsFlushed;

      public Data(TableMetadata tableMetadata, boolean isTruncate, FlushReason reason, Memtable memtable, List<SSTableReader> sstables, long durationInMillis) {
         this.keyspace = tableMetadata.keyspace;
         this.table = tableMetadata.name;
         this.reason = reason;
         this.truncate = isTruncate;
         this.memtableMemoryUsage = memtable.getMemoryUsage();
         this.memtableLiveDataSize = memtable.getLiveDataSize();
         this.flushDurationMillis = durationInMillis;
         long totalBytesOnDisk = 0L;
         int numSSTables = 0;
         long totalPartitonsFlushed = 0L;
         Iterator var13 = sstables.iterator();

         while(var13.hasNext()) {
            SSTableReader sstable = (SSTableReader)var13.next();
            if(sstable != null) {
               totalPartitonsFlushed += sstable.estimatedKeys();
               totalBytesOnDisk += sstable.bytesOnDisk();
               ++numSSTables;
            }
         }

         this.partitonsFlushed = totalPartitonsFlushed;
         this.sstableBytesOnDisk = totalBytesOnDisk;
         this.sstablesFlushed = numSSTables;
      }
   }
}
