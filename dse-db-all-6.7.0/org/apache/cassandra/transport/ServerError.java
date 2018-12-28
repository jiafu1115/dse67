package org.apache.cassandra.transport;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.TransportException;

public class ServerError extends RuntimeException implements TransportException {
   public ServerError(Throwable e) {
      super(e.toString());
   }

   public ServerError(String msg) {
      super(msg);
   }

   public ExceptionCode code() {
      return ExceptionCode.SERVER_ERROR;
   }
}
