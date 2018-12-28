package org.apache.cassandra.utils.memory;

public class SlabPool extends MemtablePool {
   final boolean allocateOnHeap;

   public SlabPool(long maxMemory, double cleanupThreshold, Runnable cleaner, boolean allocateOnHeap) {
      super(maxMemory, cleanupThreshold, cleaner);
      this.allocateOnHeap = allocateOnHeap;
   }

   public MemtableAllocator newAllocator(int coreId) {
      return new SlabAllocator(this, this.onHeap.newAllocator(), this.offHeap.newAllocator(), this.allocateOnHeap, coreId);
   }
}
