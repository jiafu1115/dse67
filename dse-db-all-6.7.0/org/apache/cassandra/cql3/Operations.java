package org.apache.cassandra.cql3;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.StatementType;

public final class Operations implements Iterable<Operation> {
   private final StatementType type;
   private final List<Operation> regularOperations = new ArrayList();
   private final List<Operation> staticOperations = new ArrayList();

   public Operations(StatementType type) {
      this.type = type;
   }

   public boolean appliesToStaticColumns() {
      return !this.staticOperations.isEmpty();
   }

   public boolean appliesToRegularColumns() {
      return !this.regularOperations.isEmpty() || this.type.isDelete() && this.staticOperations.isEmpty();
   }

   public List<Operation> regularOperations() {
      return this.regularOperations;
   }

   public List<Operation> staticOperations() {
      return this.staticOperations;
   }

   public void add(Operation operation) {
      if(operation.column.isStatic()) {
         this.staticOperations.add(operation);
      } else {
         this.regularOperations.add(operation);
      }

   }

   public boolean isEmpty() {
      return this.staticOperations.isEmpty() && this.regularOperations.isEmpty();
   }

   public Iterator<Operation> iterator() {
      return Iterators.concat(this.staticOperations.iterator(), this.regularOperations.iterator());
   }

   public void addFunctionsTo(List<Function> functions) {
      int i = 0;

      int size;
      for(size = this.regularOperations.size(); i < size; ++i) {
         ((Operation)this.regularOperations.get(i)).addFunctionsTo(functions);
      }

      i = 0;

      for(size = this.staticOperations.size(); i < size; ++i) {
         ((Operation)this.staticOperations.get(i)).addFunctionsTo(functions);
      }

   }

   public void forEachFunction(Consumer<Function> consumer) {
      int i = 0;

      int size;
      for(size = this.regularOperations.size(); i < size; ++i) {
         ((Operation)this.regularOperations.get(i)).forEachFunction(consumer);
      }

      i = 0;

      for(size = this.staticOperations.size(); i < size; ++i) {
         ((Operation)this.staticOperations.get(i)).forEachFunction(consumer);
      }

   }
}
