package org.apache.cassandra.db;

public enum WriteType {
   SIMPLE,
   BATCH,
   UNLOGGED_BATCH,
   COUNTER,
   BATCH_LOG,
   CAS,
   VIEW;

   private WriteType() {
   }
}
