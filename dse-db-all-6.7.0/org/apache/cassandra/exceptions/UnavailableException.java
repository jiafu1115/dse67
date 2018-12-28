package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;

public class UnavailableException extends RequestExecutionException {
   public final ConsistencyLevel consistency;
   public final int required;
   public final int alive;

   public UnavailableException(ConsistencyLevel consistency, int required, int alive) {
      this("Cannot achieve consistency level " + consistency, consistency, required, alive);
   }

   public UnavailableException(ConsistencyLevel consistency, String dc, int required, int alive) {
      this("Cannot achieve consistency level " + consistency + " in DC " + dc, consistency, required, alive);
   }

   public UnavailableException(String msg, ConsistencyLevel consistency, int required, int alive) {
      super(ExceptionCode.UNAVAILABLE, msg);
      this.consistency = consistency;
      this.required = required;
      this.alive = alive;
   }
}
