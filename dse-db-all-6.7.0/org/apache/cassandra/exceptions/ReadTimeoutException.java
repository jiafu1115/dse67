package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;

public class ReadTimeoutException extends RequestTimeoutException {
   public final boolean dataPresent;

   public ReadTimeoutException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent) {
      super(ExceptionCode.READ_TIMEOUT, consistency, received, blockFor);
      this.dataPresent = dataPresent;
   }

   public ReadTimeoutException(ConsistencyLevel consistency) {
      super(ExceptionCode.READ_TIMEOUT, consistency);
      this.dataPresent = false;
   }
}
