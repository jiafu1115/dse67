package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

public final class HeapAllocator extends AbstractAllocator {
   public static final HeapAllocator instance = new HeapAllocator();

   private HeapAllocator() {
   }

   public ByteBuffer allocate(int size) {
      return ByteBuffer.allocate(size);
   }

   public boolean allocatingOnHeap() {
      return true;
   }
}
