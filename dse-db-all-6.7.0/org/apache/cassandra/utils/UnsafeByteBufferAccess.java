package org.apache.cassandra.utils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class UnsafeByteBufferAccess {
   public static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
   public static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
   public static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
   public static final long BYTE_BUFFER_OFFSET_OFFSET;
   public static final long BYTE_BUFFER_HB_OFFSET;
   public static final long BYTE_BUFFER_NATIVE_ORDER;
   public static final long BYTE_BUFFER_BIG_ENDIAN;
   public static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
   public static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
   public static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
   public static final long BYTE_ARRAY_BASE_OFFSET;

   public UnsafeByteBufferAccess() {
   }

   public static long getAddress(ByteBuffer buffer) {
      return UnsafeAccess.UNSAFE.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
   }

   public static Object getArray(ByteBuffer buffer) {
      return UnsafeAccess.UNSAFE.getObject(buffer, BYTE_BUFFER_HB_OFFSET);
   }

   public static int getOffset(ByteBuffer buffer) {
      return UnsafeAccess.UNSAFE.getInt(buffer, BYTE_BUFFER_OFFSET_OFFSET);
   }

   public static boolean nativeByteOrder(ByteBuffer buffer) {
      return UnsafeAccess.UNSAFE.getBoolean(buffer, BYTE_BUFFER_NATIVE_ORDER);
   }

   public static boolean bigEndian(ByteBuffer buffer) {
      return UnsafeAccess.UNSAFE.getBoolean(buffer, BYTE_BUFFER_BIG_ENDIAN);
   }

   public static Object getAttachment(ByteBuffer instance) {
      assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;

      return UnsafeAccess.UNSAFE.getObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET);
   }

   public static void setAttachment(ByteBuffer instance, Object next) {
      assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;

      UnsafeAccess.UNSAFE.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, next);
   }

   static long bufferOffset(ByteBuffer buffer, Object array) {
      long srcOffset;
      if(array != null) {
         srcOffset = BYTE_ARRAY_BASE_OFFSET + (long)getOffset(buffer);
      } else {
         srcOffset = getAddress(buffer);
      }

      return srcOffset;
   }

   public static short getShort(ByteBuffer bb) {
      Object array = getArray(bb);
      long srcOffset = (long)bb.position() + bufferOffset(bb, array);
      if(Architecture.IS_UNALIGNED) {
         short x = UnsafeAccess.UNSAFE.getShort(array, srcOffset);
         return nativeByteOrder(bb)?x:Short.reverseBytes(x);
      } else {
         return UnsafeMemoryAccess.getShortByByte(array, srcOffset, bigEndian(bb));
      }
   }

   public static int getInt(ByteBuffer bb) {
      Object array = getArray(bb);
      long srcOffset = (long)bb.position() + bufferOffset(bb, array);
      if(Architecture.IS_UNALIGNED) {
         int x = UnsafeAccess.UNSAFE.getInt(array, srcOffset);
         return nativeByteOrder(bb)?x:Integer.reverseBytes(x);
      } else {
         return UnsafeMemoryAccess.getIntByByte(array, srcOffset, bigEndian(bb));
      }
   }

   public static long getLong(ByteBuffer bb) {
      Object array = getArray(bb);
      long srcOffset = (long)bb.position() + bufferOffset(bb, array);
      if(Architecture.IS_UNALIGNED) {
         long l = UnsafeAccess.UNSAFE.getLong(array, srcOffset);
         return nativeByteOrder(bb)?l:Long.reverseBytes(l);
      } else {
         return UnsafeMemoryAccess.getLongByByte(array, srcOffset, bigEndian(bb));
      }
   }

   public static double getDouble(ByteBuffer bb) {
      return Double.longBitsToDouble(getLong(bb));
   }

   public static float getFloat(ByteBuffer bb) {
      return Float.intBitsToFloat(getInt(bb));
   }

   public static ByteBuffer allocateByteBuffer(long address, int length) {
      return allocateByteBuffer(address, length, ByteOrder.nativeOrder());
   }

   public static ByteBuffer allocateByteBuffer(long address, int length, ByteOrder order) {
      ByteBuffer instance = allocateHollowDirectByteBuffer(order);
      initByteBufferInstance(instance, address, length);
      return instance;
   }

   public static ByteBuffer allocateHollowDirectByteBuffer() {
      return allocateHollowDirectByteBuffer(ByteOrder.nativeOrder());
   }

   public static ByteBuffer allocateHollowDirectByteBuffer(ByteOrder order) {
      ByteBuffer instance;
      try {
         instance = (ByteBuffer)UnsafeAccess.UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
      } catch (InstantiationException var3) {
         throw new AssertionError(var3);
      }

      instance.order(order);
      return instance;
   }

   public static void initByteBufferInstance(ByteBuffer instance, long address, int length) {
      setAddress(instance, address);
      setCapacity(instance, length);
      setLimit(instance, length);
   }

   public static void initByteBufferInstance(ByteBuffer instance, long address, int length, ByteOrder order) {
      initByteBufferInstance(instance, address, length);
      instance.order(order);
   }

   public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer) {
      assert source.isDirect() && hollowBuffer.isDirect();

      setAddress(hollowBuffer, getAddress(source));
      setPosition(hollowBuffer, getPosition(source));
      setLimit(hollowBuffer, getLimit(source));
      setCapacity(hollowBuffer, getCapacity(source));
      return hollowBuffer;
   }

   private static int getCapacity(ByteBuffer source) {
      return UnsafeAccess.UNSAFE.getInt(source, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET);
   }

   private static int getLimit(ByteBuffer source) {
      return UnsafeAccess.UNSAFE.getInt(source, DIRECT_BYTE_BUFFER_LIMIT_OFFSET);
   }

   private static void setPosition(ByteBuffer hollowBuffer, int position) {
      UnsafeAccess.UNSAFE.putInt(hollowBuffer, DIRECT_BYTE_BUFFER_POSITION_OFFSET, position);
   }

   private static int getPosition(ByteBuffer source) {
      return UnsafeAccess.UNSAFE.getInt(source, DIRECT_BYTE_BUFFER_POSITION_OFFSET);
   }

   private static void setLimit(ByteBuffer instance, int length) {
      UnsafeAccess.UNSAFE.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
   }

   private static void setCapacity(ByteBuffer instance, int length) {
      UnsafeAccess.UNSAFE.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
   }

   private static void setAddress(ByteBuffer instance, long address) {
      UnsafeAccess.UNSAFE.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
   }

   static {
      try {
         DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
         DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
         DIRECT_BYTE_BUFFER_LIMIT_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
         DIRECT_BYTE_BUFFER_POSITION_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("position"));
         DIRECT_BYTE_BUFFER_CLASS = ByteBuffer.allocateDirect(0).getClass();
         DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(DIRECT_BYTE_BUFFER_CLASS.getDeclaredField("att"));
         BYTE_BUFFER_OFFSET_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
         BYTE_BUFFER_HB_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
         BYTE_BUFFER_NATIVE_ORDER = UnsafeAccess.UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("nativeByteOrder"));
         BYTE_BUFFER_BIG_ENDIAN = UnsafeAccess.UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("bigEndian"));
         BYTE_ARRAY_BASE_OFFSET = (long)UnsafeAccess.UNSAFE.arrayBaseOffset(byte[].class);
      } catch (Exception var1) {
         throw new AssertionError(var1);
      }
   }
}
