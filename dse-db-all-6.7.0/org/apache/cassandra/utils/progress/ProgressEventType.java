package org.apache.cassandra.utils.progress;

public enum ProgressEventType {
   START,
   PROGRESS,
   ERROR,
   ABORT,
   SUCCESS,
   COMPLETE,
   NOTIFICATION;

   private ProgressEventType() {
   }
}
