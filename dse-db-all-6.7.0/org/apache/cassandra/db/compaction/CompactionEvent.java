package org.apache.cassandra.db.compaction;

public enum CompactionEvent {
   STARTED,
   ENDED;

   private CompactionEvent() {
   }
}
