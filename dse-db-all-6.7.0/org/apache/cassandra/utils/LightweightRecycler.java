package org.apache.cassandra.utils;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

public final class LightweightRecycler<T> extends FastThreadLocal<ArrayDeque<T>> {
   private final int capacity;

   public LightweightRecycler(int capacity) {
      this.capacity = capacity;
   }

   protected ArrayDeque<T> initialValue() throws Exception {
      return new ArrayDeque(this.capacity);
   }

   public T reuse() {
      return ((ArrayDeque)this.get()).pollFirst();
   }

   public T reuseOrAllocate(Supplier<T> supplier) {
      T reuse = this.reuse();
      return reuse != null?reuse:supplier.get();
   }

   public boolean tryRecycle(T t) {
      Objects.requireNonNull(t);
      ArrayDeque<T> pool = (ArrayDeque)this.get();
      if(pool.size() < this.capacity) {
         if(t instanceof Collection) {
            ((Collection)t).clear();
         }

         pool.offerFirst(t);
         return true;
      } else {
         return false;
      }
   }

   public int capacity() {
      return this.capacity;
   }

   public int available() {
      return ((ArrayDeque)this.get()).size();
   }
}
