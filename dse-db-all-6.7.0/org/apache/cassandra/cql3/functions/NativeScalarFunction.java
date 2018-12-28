package org.apache.cassandra.cql3.functions;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class NativeScalarFunction extends NativeFunction implements ScalarFunction {
   protected NativeScalarFunction(String name, AbstractType<?> returnType, AbstractType... argsType) {
      super(name, returnType, argsType);
   }

   public boolean isCalledOnNullInput() {
      return true;
   }

   public final boolean isAggregate() {
      return false;
   }
}
