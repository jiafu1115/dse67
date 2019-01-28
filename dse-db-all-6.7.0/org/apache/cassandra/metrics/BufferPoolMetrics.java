package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.utils.memory.BufferPool;

public class BufferPoolMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("BufferPool");
   public final Meter misses;
   public final Gauge<Long> totalSize;
   public final Gauge<Long> overflowSize;
   public final Gauge<Long> usedSize;
   public final Gauge<Long> chunkReaderBufferSize;

   public BufferPoolMetrics() {
      this.misses = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Misses"));
      this.totalSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Size"), BufferPool::sizeInBytes);
      this.usedSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("UsedSize"), BufferPool::usedSizeInBytes);
      this.overflowSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("OverflowSize"), BufferPool::sizeInBytesOverLimit);
      this.chunkReaderBufferSize = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("ChunkReaderBufferSize"), ChunkReader.bufferSize::get);
   }
}
