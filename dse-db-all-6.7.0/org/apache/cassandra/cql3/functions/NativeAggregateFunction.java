package org.apache.cassandra.cql3.functions;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class NativeAggregateFunction extends NativeFunction implements AggregateFunction {
   protected NativeAggregateFunction(String name, AbstractType<?> returnType, AbstractType... argTypes) {
      super(name, returnType, argTypes);
   }

   public final boolean isAggregate() {
      return true;
   }
}
