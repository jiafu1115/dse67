package org.apache.cassandra.cql3.selection;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

abstract class MutableTimestamps implements Timestamps {
   protected final MutableTimestamps.TimestampsType type;

   protected MutableTimestamps(MutableTimestamps.TimestampsType type) {
      this.type = type;
   }

   public MutableTimestamps.TimestampsType type() {
      return this.type;
   }

   public abstract void capacity(int var1);

   public abstract void addNoTimestamp();

   public abstract void addTimestampFrom(Cell var1, int var2);

   public abstract void reset();

   static MutableTimestamps newTimestamps(MutableTimestamps.TimestampsType timestampType, AbstractType<?> columnType) {
      return (MutableTimestamps)(!columnType.isMultiCell()?new MutableTimestamps.SingleTimestamps(timestampType):(columnType instanceof UserType?new MutableTimestamps.MultipleTimestamps(timestampType, ((UserType)columnType).size()):new MutableTimestamps.MultipleTimestamps(timestampType, 0)));
   }

   private static final class SingleTimestamps extends MutableTimestamps {
      private long timestamp;

      public SingleTimestamps(MutableTimestamps.TimestampsType type) {
         this(type, type.defaultValue());
      }

      public SingleTimestamps(MutableTimestamps.TimestampsType type, long timestamp) {
         super(type);
         this.timestamp = timestamp;
      }

      public void addNoTimestamp() {
         this.timestamp = this.type.defaultValue();
      }

      public void addTimestampFrom(Cell cell, int nowInSecond) {
         this.timestamp = this.type.getTimestamp(cell, nowInSecond);
      }

      public void capacity(int newCapacity) {
         throw new UnsupportedOperationException();
      }

      public void reset() {
         this.timestamp = this.type.defaultValue();
      }

      public Timestamps get(int index) {
         return this;
      }

      public Timestamps slice(Range<Integer> range) {
         return (Timestamps)(range.isEmpty()?NO_TIMESTAMP:this);
      }

      public int size() {
         return 1;
      }

      public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion) {
         return this.timestamp == this.type.defaultValue()?null:this.type.toByteBuffer(this.timestamp);
      }

      public String toString() {
         return this.type + ": " + Long.toString(this.timestamp);
      }
   }

   private static final class MultipleTimestamps extends MutableTimestamps {
      private long[] timestamps;
      private int offset;
      private int capacity;
      private int index;

      public MultipleTimestamps(MutableTimestamps.TimestampsType type, int initialCapacity) {
         this(type, new long[initialCapacity], 0, initialCapacity);
      }

      public MultipleTimestamps(MutableTimestamps.TimestampsType type, long[] timestamps, int offset, int initialCapacity) {
         super(type);
         this.timestamps = timestamps;
         this.offset = offset;
         this.capacity = initialCapacity;
      }

      public void addNoTimestamp() {
         this.validateIndex(this.index);
         this.timestamps[this.offset + this.index++] = this.type.defaultValue();
      }

      public void addTimestampFrom(Cell cell, int nowInSecond) {
         this.validateIndex(this.index);
         this.timestamps[this.offset + this.index++] = this.type.getTimestamp(cell, nowInSecond);
      }

      public void capacity(int newCapacity) {
         if(this.offset != 0 || this.timestamps.length != newCapacity) {
            this.timestamps = new long[newCapacity];
            this.offset = 0;
            this.capacity = newCapacity;
         }

      }

      public void reset() {
         this.index = 0;
      }

      public Timestamps get(int index) {
         return (Timestamps)(index < 0 && index >= this.capacity?NO_TIMESTAMP:new MutableTimestamps.SingleTimestamps(this.type, this.timestamps[this.offset + index]));
      }

      public int size() {
         return this.index;
      }

      public Timestamps slice(Range<Integer> range) {
         if(range.isEmpty()) {
            return NO_TIMESTAMP;
         } else {
            int from = !range.hasLowerBound()?0:(range.lowerBoundType() == BoundType.CLOSED?((Integer)range.lowerEndpoint()).intValue():((Integer)range.lowerEndpoint()).intValue() + 1);
            int to = !range.hasUpperBound()?this.size() - 1:(range.upperBoundType() == BoundType.CLOSED?((Integer)range.upperEndpoint()).intValue():((Integer)range.upperEndpoint()).intValue() - 1);
            int sliceSize = to - from + 1;
            MutableTimestamps.MultipleTimestamps slice = new MutableTimestamps.MultipleTimestamps(this.type, this.timestamps, from, sliceSize);
            slice.index = sliceSize;
            return slice;
         }
      }

      public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion) {
         List<ByteBuffer> buffers = new ArrayList(this.index);
         int i = 0;

         for(int m = this.size(); i < m; ++i) {
            long timestamp = this.timestamps[this.offset + i];
            buffers.add(this.type.toByteBuffer(timestamp));
         }

         return buffers.isEmpty()?null:CollectionSerializer.pack(buffers, this.capacity, protocolVersion);
      }

      private void validateIndex(int index) {
         if(index < 0 && index >= this.capacity) {
            throw new IndexOutOfBoundsException("index: " + index + " capacity: " + this.capacity);
         }
      }

      public String toString() {
         return this.type + ": " + Arrays.toString(Arrays.copyOfRange(this.timestamps, this.offset, this.index));
      }
   }

   public static enum TimestampsType {
      WRITETIMESTAMPS {
         long getTimestamp(Cell cell, int nowInSecond) {
            return cell.timestamp();
         }

         long defaultValue() {
            return -9223372036854775808L;
         }

         ByteBuffer toByteBuffer(long timestamp) {
            return timestamp == this.defaultValue()?null:ByteBufferUtil.bytes(timestamp);
         }
      },
      TTLS {
         long getTimestamp(Cell cell, int nowInSecond) {
            if(!cell.isExpiring()) {
               return this.defaultValue();
            } else {
               int remaining = cell.localDeletionTime() - nowInSecond;
               return remaining >= 0?(long)remaining:this.defaultValue();
            }
         }

         long defaultValue() {
            return -1L;
         }

         ByteBuffer toByteBuffer(long timestamp) {
            return timestamp == this.defaultValue()?null:ByteBufferUtil.bytes((int)timestamp);
         }
      };

      private TimestampsType() {
      }

      abstract long getTimestamp(Cell var1, int var2);

      abstract long defaultValue();

      abstract ByteBuffer toByteBuffer(long var1);
   }
}
