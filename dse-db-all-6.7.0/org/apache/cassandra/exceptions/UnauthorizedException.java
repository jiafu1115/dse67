package org.apache.cassandra.exceptions;

public class UnauthorizedException extends RequestValidationException {
   public UnauthorizedException(String msg) {
      super(ExceptionCode.UNAUTHORIZED, msg);
   }
}
