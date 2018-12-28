package org.apache.cassandra.exceptions;

public abstract class CassandraException extends RuntimeException implements TransportException {
   private final ExceptionCode code;

   protected CassandraException(ExceptionCode code, String msg) {
      super(msg);
      this.code = code;
   }

   protected CassandraException(ExceptionCode code, String msg, Throwable cause) {
      super(msg, cause);
      this.code = code;
   }

   public ExceptionCode code() {
      return this.code;
   }
}
