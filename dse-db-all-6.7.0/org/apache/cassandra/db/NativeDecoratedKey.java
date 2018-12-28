package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeDecoratedKey extends DecoratedKey {
   final long peer;

   public NativeDecoratedKey(Token token, NativeAllocator allocator, ByteBuffer key) {
      super(token);

      assert key != null;

      assert key.order() == ByteOrder.BIG_ENDIAN;

      int size = key.remaining();
      this.peer = allocator.allocate(4 + size);
      UnsafeMemoryAccess.setInt(this.peer, size);
      UnsafeCopy.copyBufferToMemory(key, this.address());
   }

   int length() {
      return UnsafeMemoryAccess.getInt(this.peer);
   }

   long address() {
      return this.peer + 4L;
   }

   public ByteBuffer getKey() {
      return UnsafeByteBufferAccess.allocateByteBuffer(this.address(), this.length(), ByteOrder.BIG_ENDIAN);
   }

   public ByteBuffer cloneKey(AbstractAllocator allocator) {
      long srcAddress = this.address();
      int length = this.length();
      ByteBuffer cloney = allocator.allocate(length);
      UnsafeCopy.copyMemoryToBuffer(srcAddress, cloney, length);
      return cloney;
   }
}
