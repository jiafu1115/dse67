package org.apache.cassandra.exceptions;

public class SyntaxException extends RequestValidationException {
   public SyntaxException(String msg) {
      super(ExceptionCode.SYNTAX_ERROR, msg);
   }
}
