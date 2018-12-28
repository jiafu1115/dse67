package org.apache.cassandra.exceptions;

public class OverloadedException extends RequestExecutionException {
   public OverloadedException(String reason) {
      super(ExceptionCode.OVERLOADED, reason);
   }
}
