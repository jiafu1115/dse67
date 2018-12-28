package org.apache.cassandra.cql3.conditions;

import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.schema.ColumnMetadata;

abstract class AbstractConditions implements Conditions {
   AbstractConditions() {
   }

   public void addFunctionsTo(List<Function> functions) {
   }

   public void forEachFunction(Consumer<Function> consumer) {
   }

   public Iterable<ColumnMetadata> getColumns() {
      return null;
   }

   public boolean isEmpty() {
      return false;
   }

   public boolean appliesToStaticColumns() {
      return false;
   }

   public boolean appliesToRegularColumns() {
      return false;
   }

   public boolean isIfExists() {
      return false;
   }

   public boolean isIfNotExists() {
      return false;
   }
}
