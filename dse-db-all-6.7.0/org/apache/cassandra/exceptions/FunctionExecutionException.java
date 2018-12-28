package org.apache.cassandra.exceptions;

import java.util.List;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;

public class FunctionExecutionException extends RequestExecutionException {
   public final FunctionName functionName;
   public final List<String> argTypes;
   public final String detail;

   public static FunctionExecutionException create(Function function, Throwable cause) {
      List<String> cqlTypes = AbstractType.asCQLTypeStringList(function.argTypes());
      FunctionExecutionException fee = new FunctionExecutionException(function.name(), cqlTypes, cause.toString());
      fee.initCause(cause);
      return fee;
   }

   public FunctionExecutionException(FunctionName functionName, List<String> argTypes, String detail) {
      super(ExceptionCode.FUNCTION_FAILURE, "execution of '" + functionName + argTypes + "' failed: " + detail);
      this.functionName = functionName;
      this.argTypes = argTypes;
      this.detail = detail;
   }
}
