package org.apache.cassandra.exceptions;

public class IsBootstrappingException extends RequestExecutionException {
   public IsBootstrappingException() {
      super(ExceptionCode.IS_BOOTSTRAPPING, "Cannot read from a bootstrapping node");
   }
}
