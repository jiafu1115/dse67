package org.apache.cassandra.cql3.continuous.paging;

import org.apache.cassandra.exceptions.ClientWriteException;
import org.apache.cassandra.metrics.ContinuousPagingMetrics;
import org.apache.cassandra.metrics.LatencyMetrics;

public class ContinuousPagingMetricsEventHandler implements ContinuousPagingEventHandler {
   protected final ContinuousPagingMetrics metrics;
   protected final LatencyMetrics completionLatency;

   public ContinuousPagingMetricsEventHandler(ContinuousPagingMetrics metrics, LatencyMetrics completionLatency) {
      this.metrics = metrics;
      this.completionLatency = completionLatency;
   }

   public void sessionCreated(Throwable error) {
      if(error != null) {
         if(error instanceof TooManyContinuousPagingSessions) {
            this.metrics.tooManySessions.mark();
         }

         this.metrics.creationFailures.mark();
      }
   }

   public void sessionExecuting(long timeInQueue) {
      this.metrics.waitingTime.addNano(timeInQueue);
   }

   public void sessionCompleted(long duration, Throwable error) {
      if(error == null) {
         this.completionLatency.addNano(duration);
      } else if(error instanceof ClientWriteException) {
         this.metrics.clientWriteExceptions.mark();
      } else {
         this.metrics.failures.mark();
      }

   }

   public void sessionRemoved() {
   }

   public void sessionPaused() {
      this.metrics.serverBlocked.inc();
   }

   public void sessionResumed(long timePaused) {
      this.metrics.serverBlockedLatency.addNano(timePaused);
   }
}
