package org.apache.cassandra.utils.memory;

public class NativePool extends MemtablePool {
   public NativePool(long maxMemory, double cleanThreshold, Runnable cleaner) {
      super(maxMemory, cleanThreshold, cleaner);
   }

   public NativeAllocator newAllocator(int coreId) {
      return new NativeAllocator(this, coreId);
   }
}
