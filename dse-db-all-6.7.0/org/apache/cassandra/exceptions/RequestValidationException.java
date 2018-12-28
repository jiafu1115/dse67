package org.apache.cassandra.exceptions;

public abstract class RequestValidationException extends CassandraException {
   protected RequestValidationException(ExceptionCode code, String msg) {
      super(code, msg);
   }

   protected RequestValidationException(ExceptionCode code, String msg, Throwable e) {
      super(code, msg, e);
   }
}
