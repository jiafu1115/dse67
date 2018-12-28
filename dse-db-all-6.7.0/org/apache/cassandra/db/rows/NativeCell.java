package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCell extends AbstractCell {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeCell());
   private static final long HAS_CELLPATH = 0L;
   private static final long TIMESTAMP = 1L;
   private static final long TTL = 9L;
   private static final long DELETION = 13L;
   private static final long LENGTH = 17L;
   private static final long VALUE = 21L;
   private final long peer;

   private NativeCell() {
      super((ColumnMetadata)null);
      this.peer = 0L;
   }

   public NativeCell(NativeAllocator allocator, Cell cell) {
      this(allocator, cell.column(), cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cell.value(), cell.path());
   }

   public NativeCell(NativeAllocator allocator, ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, ByteBuffer value, CellPath path) {
      super(column);
      long size = simpleSize(value.remaining());

      assert value.order() == ByteOrder.BIG_ENDIAN;

      assert column.isComplex() == (path != null);

      if(path != null) {
         assert path.size() == 1;

         size += (long)(4 + path.get(0).remaining());
      }

      if(size > 2147483647L) {
         throw new IllegalStateException();
      } else {
         this.peer = allocator.allocate((int)size);
         UnsafeMemoryAccess.setByte(this.peer + 0L, (byte)(path == null?0:1));
         UnsafeMemoryAccess.setLong(this.peer + 1L, timestamp);
         UnsafeMemoryAccess.setInt(this.peer + 9L, ttl);
         UnsafeMemoryAccess.setInt(this.peer + 13L, localDeletionTime);
         UnsafeMemoryAccess.setInt(this.peer + 17L, value.remaining());
         UnsafeCopy.copyBufferToMemory(value, this.valueAddress());
         if(path != null) {
            ByteBuffer pathbuffer = path.get(0);

            assert pathbuffer.order() == ByteOrder.BIG_ENDIAN;

            long offset = this.peer + 21L + (long)value.remaining();
            UnsafeMemoryAccess.setInt(offset, pathbuffer.remaining());
            UnsafeCopy.copyBufferToMemory(pathbuffer, offset + 4L);
         }

      }
   }

   private static long simpleSize(int length) {
      return 21L + (long)length;
   }

   public long timestamp() {
      return UnsafeMemoryAccess.getLong(this.peer + 1L);
   }

   public int ttl() {
      return UnsafeMemoryAccess.getInt(this.peer + 9L);
   }

   public int localDeletionTime() {
      return UnsafeMemoryAccess.getInt(this.peer + 13L);
   }

   public ByteBuffer value() {
      return UnsafeByteBufferAccess.allocateByteBuffer(this.valueAddress(), this.valueLength(), ByteOrder.BIG_ENDIAN);
   }

   public ByteBuffer value(ByteBuffer reusableFlyWeight) {
      if(reusableFlyWeight == null) {
         return this.value();
      } else {
         UnsafeByteBufferAccess.initByteBufferInstance(reusableFlyWeight, this.valueAddress(), this.valueLength(), ByteOrder.BIG_ENDIAN);
         return reusableFlyWeight;
      }
   }

   protected ByteBuffer cloneValue(AbstractAllocator allocator) {
      long srcAddress = this.valueAddress();
      int length = this.valueLength();
      ByteBuffer cloney = allocator.allocate(length);
      UnsafeCopy.copyMemoryToBuffer(srcAddress, cloney, length);
      return cloney;
   }

   public CellPath path() {
      if(UnsafeMemoryAccess.getByte(this.peer + 0L) == 0) {
         return null;
      } else {
         long offset = this.peer + 21L + (long)this.valueLength();
         int size = UnsafeMemoryAccess.getInt(offset);
         return CellPath.create(UnsafeByteBufferAccess.allocateByteBuffer(offset + 4L, size, ByteOrder.BIG_ENDIAN));
      }
   }

   public Cell withUpdatedValue(ByteBuffer newValue) {
      throw new UnsupportedOperationException();
   }

   public Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime) {
      return new BufferCell(this.column, newTimestamp, this.ttl(), newLocalDeletionTime, this.value(), this.path());
   }

   public Cell withUpdatedColumn(ColumnMetadata column) {
      return new BufferCell(column, this.timestamp(), this.ttl(), this.localDeletionTime(), this.value(), this.path());
   }

   public Cell withSkippedValue() {
      return new BufferCell(this.column, this.timestamp(), this.ttl(), this.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, this.path());
   }

   public Cell withUpdatedColumnNoValue(ColumnMetadata column) {
      return new BufferCell(column, this.timestamp(), this.ttl(), this.localDeletionTime(), ByteBufferUtil.EMPTY_BYTE_BUFFER, this.path());
   }

   public long unsharedHeapSizeExcludingData() {
      return EMPTY_SIZE;
   }

   private long valueAddress() {
      return this.peer + 21L;
   }

   protected int valueLength() {
      return UnsafeMemoryAccess.getInt(this.peer + 17L);
   }
}
