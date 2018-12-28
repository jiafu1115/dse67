package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

public class WriteTimeoutException extends RequestTimeoutException {
   public final WriteType writeType;

   public WriteTimeoutException(WriteType writeType, ConsistencyLevel consistency, int received, int blockFor) {
      super(ExceptionCode.WRITE_TIMEOUT, consistency, received, blockFor);
      this.writeType = writeType;
   }

   public WriteTimeoutException(WriteType writeType, String message, ConsistencyLevel consistency, int received, int blockFor) {
      super(ExceptionCode.WRITE_TIMEOUT, message, consistency, received, blockFor);
      this.writeType = writeType;
   }
}
