package org.apache.cassandra.db.monitoring;

import io.reactivex.functions.Function;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.time.ApolloTime;

public class Monitor {
   private static final int TEST_ITERATION_DELAY_MILLIS = PropertyConfiguration.getInteger("cassandra.test.read_iteration_delay_ms", 0);
   final Monitorable operation;
   private final boolean withReporting;
   final long operationCreationTimeMillis;
   private final long monitoringStartTimeMillis;
   final long timeoutMillis;
   final long slowQueryTimeoutMillis;
   final boolean isLocalOperation;
   private MonitoringState state;
   private boolean isSlow;
   private long lastChecked;

   private Monitor(Monitorable operation, boolean withReporting, long operationCreationTimeMillis, long monitoringStartTimeMillis, long timeoutMillis, long slowQueryTimeoutMillis, boolean isLocalOperation) {
      this.state = MonitoringState.IN_PROGRESS;
      this.operation = operation;
      this.withReporting = withReporting;
      this.operationCreationTimeMillis = operationCreationTimeMillis;
      this.monitoringStartTimeMillis = monitoringStartTimeMillis;
      this.timeoutMillis = timeoutMillis;
      this.slowQueryTimeoutMillis = slowQueryTimeoutMillis;
      this.isLocalOperation = isLocalOperation;
   }

   public static boolean isTesting() {
      return TEST_ITERATION_DELAY_MILLIS != 0;
   }

   public static Monitor createAndStart(Monitorable operation, long operationCreationTimeMillis, long timeoutMillis, boolean isLocalOperation) {
      return createAndStart(operation, operationCreationTimeMillis, timeoutMillis, isLocalOperation, DatabaseDescriptor.getSlowQueryTimeout());
   }

   public static Monitor createAndStart(Monitorable operation, long operationCreationTimeMillis, long timeoutMillis, boolean isLocalOperation, long slowQueryTimeout) {
      return new Monitor(operation, true, operationCreationTimeMillis, ApolloTime.systemClockMillis(), timeoutMillis, slowQueryTimeout, isLocalOperation);
   }

   public static Monitor createAndStartNoReporting(Monitorable operation, long operationCreationTimeMillis, long timeoutMillis) {
      return new Monitor(operation, false, operationCreationTimeMillis, ApolloTime.systemClockMillis(), timeoutMillis, 0L, false);
   }

   long timeoutMillis() {
      return this.timeoutMillis;
   }

   long slowQueryTimeoutMillis() {
      return this.slowQueryTimeoutMillis;
   }

   boolean isInProgress() {
      this.checkSilently();
      return this.state == MonitoringState.IN_PROGRESS;
   }

   boolean isCompleted() {
      this.checkSilently();
      return this.state == MonitoringState.COMPLETED;
   }

   boolean isAborted() {
      this.checkSilently();
      return this.state == MonitoringState.ABORTED;
   }

   boolean isSlow() {
      this.checkSilently();
      return this.isSlow;
   }

   private void abort() {
      switch(null.$SwitchMap$org$apache$cassandra$db$monitoring$MonitoringState[this.state.ordinal()]) {
      case 1:
         if(this.withReporting) {
            MonitoringTask.addFailedOperation(this, ApolloTime.millisTime());
         }

         this.state = MonitoringState.ABORTED;
      case 2:
         throw new AbortedOperationException();
      default:
      }
   }

   public boolean complete() {
      if(this.state == MonitoringState.IN_PROGRESS) {
         if(this.withReporting && this.isSlow && this.slowQueryTimeoutMillis > 0L) {
            MonitoringTask.addSlowOperation(this, ApolloTime.millisTime());
         }

         this.state = MonitoringState.COMPLETED;
         return true;
      } else {
         return this.state == MonitoringState.COMPLETED;
      }
   }

   private void checkSilently() {
      try {
         this.check();
      } catch (AbortedOperationException var2) {
         ;
      }

   }

   private void check() {
      if(this.state == MonitoringState.IN_PROGRESS) {
         long currentTime = ApolloTime.millisTime();
         if(currentTime - this.lastChecked >= 10L) {
            this.lastChecked = currentTime;
            if(currentTime - this.monitoringStartTimeMillis >= this.slowQueryTimeoutMillis) {
               this.isSlow = true;
            }

            if(currentTime - this.operationCreationTimeMillis >= this.timeoutMillis) {
               this.abort();
            }

         }
      }
   }

   public Flow<FlowableUnfilteredPartition> withMonitoring(Flow<FlowableUnfilteredPartition> partitions) {
      Function<Unfiltered, Unfiltered> checkForAbort = (unfiltered) -> {
         if(isTesting()) {
            FBUtilities.sleepQuietly((long)TEST_ITERATION_DELAY_MILLIS);
         }

         this.check();
         return unfiltered;
      };
      return partitions.map((partition) -> {
         this.check();
         return partition.mapContent(checkForAbort);
      });
   }
}
