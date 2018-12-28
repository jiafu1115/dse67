package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

public final class ContextAllocator extends AbstractAllocator {
   private final MemtableBufferAllocator allocator;

   public ContextAllocator(MemtableBufferAllocator allocator) {
      this.allocator = allocator;
   }

   public ByteBuffer allocate(int size) {
      return this.allocator.allocate(size);
   }
}
