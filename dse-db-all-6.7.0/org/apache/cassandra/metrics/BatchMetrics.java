package org.apache.cassandra.metrics;

public class BatchMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("Batch");
   public final com.codahale.metrics.Histogram partitionsPerLoggedBatch;
   public final com.codahale.metrics.Histogram partitionsPerUnloggedBatch;
   public final com.codahale.metrics.Histogram partitionsPerCounterBatch;

   public BatchMetrics() {
      this.partitionsPerLoggedBatch = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("PartitionsPerLoggedBatch"), false);
      this.partitionsPerUnloggedBatch = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("PartitionsPerUnloggedBatch"), false);
      this.partitionsPerCounterBatch = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("PartitionsPerCounterBatch"), false);
   }
}
