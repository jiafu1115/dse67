package org.apache.cassandra.db.monitoring;

public enum MonitoringState {
   IN_PROGRESS,
   ABORTED,
   COMPLETED;

   private MonitoringState() {
   }
}
