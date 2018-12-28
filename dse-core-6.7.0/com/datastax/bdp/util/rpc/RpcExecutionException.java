package com.datastax.bdp.util.rpc;

import com.datastax.bdp.util.DseUtil;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RpcExecutionException extends RequestExecutionException {
   private static final Logger logger = LoggerFactory.getLogger(RpcExecutionException.class);

   public RpcExecutionException(ExceptionCode exceptionCode, String msg, Throwable cause) {
      super(exceptionCode, msg, cause);
   }

   public static RpcExecutionException create(String msg, Throwable cause) {
      if(cause instanceof InvocationTargetException) {
         cause = ((InvocationTargetException)cause).getTargetException();
      }

      Optional<Throwable> ace = DseUtil.findCause(cause, AccessControlException.class);
      if(!ace.isPresent()) {
         ace = DseUtil.findCause(cause, java.security.AccessControlException.class);
      }

      if(ace.isPresent()) {
         logger.info(msg);
         return new RpcExecutionException(ExceptionCode.UNAUTHORIZED, ((Throwable)ace.get()).getMessage(), cause);
      } else {
         logger.info(msg, cause);
         return new RpcExecutionException(ExceptionCode.SERVER_ERROR, msg, cause);
      }
   }
}
