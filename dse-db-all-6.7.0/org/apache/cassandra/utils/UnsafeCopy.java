package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public abstract class UnsafeCopy {
   private static final long UNSAFE_COPY_THRESHOLD = 1048576L;
   private static final long MIN_COPY_THRESHOLD = 6L;

   public UnsafeCopy() {
   }

   public static void copyBufferToMemory(ByteBuffer srcBuffer, long tgtAddress) {
      copyBufferToMemory(srcBuffer, srcBuffer.position(), tgtAddress, srcBuffer.remaining());
   }

   public static void copyMemoryToBuffer(long srcAddress, ByteBuffer dstBuffer, int length) {
      copyMemoryToBuffer(srcAddress, dstBuffer, dstBuffer.position(), length);
   }

   public static void copyMemoryToArray(long srcAddress, byte[] dstArray, int dstOffset, int length) {
      copy0((Object)null, srcAddress, dstArray, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)dstOffset, (long)length);
   }

   public static void copyBufferToArray(ByteBuffer srcBuf, byte[] dstArray, int dstOffset) {
      copyBufferToArray(srcBuf, srcBuf.position(), dstArray, dstOffset, srcBuf.remaining());
   }

   public static void copyBufferToArray(ByteBuffer srcBuf, int srcPosition, byte[] dstArray, int dstOffset, int length) {
      if(length > dstArray.length - dstOffset) {
         throw new IllegalArgumentException("Cannot copy " + length + " bytes into array of length " + dstArray.length + " at offset " + dstOffset);
      } else {
         Object src = UnsafeByteBufferAccess.getArray(srcBuf);
         long srcOffset = UnsafeByteBufferAccess.bufferOffset(srcBuf, src) + (long)srcPosition;
         copy0(src, srcOffset, dstArray, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)dstOffset, (long)length);
      }
   }

   public static void copyBufferToMemory(ByteBuffer srcBuf, int srcPosition, long dstAddress, int length) {
      Object src = UnsafeByteBufferAccess.getArray(srcBuf);
      long srcOffset = UnsafeByteBufferAccess.bufferOffset(srcBuf, src) + (long)srcPosition;
      copy0(src, srcOffset, (Object)null, dstAddress, (long)length);
   }

   public static void copyMemoryToBuffer(long srcAddress, ByteBuffer dstBuf, int dstPosition, int length) {
      Object dst = UnsafeByteBufferAccess.getArray(dstBuf);
      long dstOffset = UnsafeByteBufferAccess.bufferOffset(dstBuf, dst) + (long)dstPosition;
      copy0((Object)null, srcAddress, dst, dstOffset, (long)length);
   }

   public static void copyArrayToMemory(byte[] src, int srcPosition, long dstAddress, int length) {
      long srcOffset = UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)srcPosition;
      copy0(src, srcOffset, (Object)null, dstAddress, (long)length);
   }

   public static void copyMemoryToMemory(long srcAddress, long dstAddress, long length) {
      copy0((Object)null, srcAddress, (Object)null, dstAddress, length);
   }

   public static void copy0(Object src, long srcOffset, Object dst, long dstOffset, long length) {
      if(length > 6L) {
         while(length > 0L) {
            long size = length > 1048576L?1048576L:length;
            UnsafeAccess.UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
            length -= size;
            srcOffset += size;
            dstOffset += size;
         }

      } else {
         for(int i = 0; (long)i < length; ++i) {
            byte b = UnsafeAccess.UNSAFE.getByte(src, srcOffset + (long)i);
            UnsafeAccess.UNSAFE.putByte(dst, dstOffset + (long)i, b);
         }

      }
   }
}
