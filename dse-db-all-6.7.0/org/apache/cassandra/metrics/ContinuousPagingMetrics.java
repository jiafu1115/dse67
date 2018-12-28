package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;

public class ContinuousPagingMetrics {
   private final MetricNameFactory factory;
   public final LatencyMetrics optimizedPathLatency;
   public final LatencyMetrics slowPathLatency;
   public final Gauge liveSessions;
   public final Gauge pendingPages;
   public final Meter requests;
   public final Meter creationFailures;
   public final Meter tooManySessions;
   public final Meter clientWriteExceptions;
   public final Meter failures;
   public final LatencyMetrics waitingTime;
   public final Counter serverBlocked;
   public final LatencyMetrics serverBlockedLatency;

   public ContinuousPagingMetrics(String familyName) {
      this.factory = new DefaultNameFactory(familyName, "");
      this.optimizedPathLatency = new LatencyMetrics(familyName, "OptimizedPathLatency");
      this.slowPathLatency = new LatencyMetrics(familyName, "SlowPathLatency");
      this.liveSessions = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("LiveSessions"), ContinuousPagingService::liveSessions);
      this.pendingPages = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("PendingPages"), ContinuousPagingService::pendingPages);
      this.requests = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Requests"));
      this.creationFailures = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("CreationFailures"));
      this.tooManySessions = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("TooManySessions"));
      this.clientWriteExceptions = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("ClientWriteExceptions"));
      this.failures = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Failures"));
      this.waitingTime = new LatencyMetrics(familyName, "WaitingTime");
      this.serverBlocked = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("ServerBlocked"));
      this.serverBlockedLatency = new LatencyMetrics(familyName, "ServerBlockedLatency");
   }

   public void release() {
      this.optimizedPathLatency.release();
      this.slowPathLatency.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("LiveSessions"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("PendingPages"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Requests"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CreationFailures"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("TooManySessions"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ClientWriteExceptions"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Failures"));
      this.waitingTime.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ServerBlocked"));
      this.serverBlockedLatency.release();
   }
}
