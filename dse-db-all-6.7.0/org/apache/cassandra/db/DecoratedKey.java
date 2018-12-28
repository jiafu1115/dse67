package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public abstract class DecoratedKey extends PartitionPosition implements IFilter.FilterKey {
   public static final Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>() {
      public int compare(DecoratedKey o1, DecoratedKey o2) {
         return o1.compareTo((PartitionPosition)o2);
      }
   };
   private int hashCode = -1;

   public DecoratedKey(Token token) {
      super(token, PartitionPosition.Kind.ROW_KEY);

      assert token != null;

   }

   public int hashCode() {
      int currHashCode = this.hashCode;
      if(currHashCode == -1) {
         currHashCode = this.getKey().hashCode();
         this.hashCode = currHashCode;
      }

      return currHashCode;
   }

   public boolean equals(Object obj) {
      if(this == obj) {
         return true;
      } else if(obj != null && obj instanceof DecoratedKey) {
         DecoratedKey other = (DecoratedKey)obj;
         return this.compareKeys(other) == 0;
      } else {
         return false;
      }
   }

   private int compareKeys(DecoratedKey other) {
      boolean isThisNative = this instanceof NativeDecoratedKey;
      boolean isOtherNative = other instanceof NativeDecoratedKey;
      NativeDecoratedKey tnKey;
      if(isOtherNative && !isThisNative) {
         tnKey = (NativeDecoratedKey)other;
         return FastByteOperations.UnsafeOperations.compare0(this.getKey(), (Object)null, tnKey.address(), tnKey.length());
      } else if(isOtherNative && isThisNative) {
         tnKey = (NativeDecoratedKey)other;
         NativeDecoratedKey tnKey = (NativeDecoratedKey)this;
         return FastByteOperations.UnsafeOperations.compare0((Object)null, tnKey.address(), tnKey.length(), (Object)null, tnKey.address(), tnKey.length());
      } else if(isThisNative) {
         tnKey = (NativeDecoratedKey)this;
         return -FastByteOperations.UnsafeOperations.compare0(other.getKey(), (Object)null, tnKey.address(), tnKey.length());
      } else {
         return ByteBufferUtil.compareUnsigned(this.getKey(), other.getKey());
      }
   }

   public int compareTo(PartitionPosition pos) {
      if(this == pos) {
         return 0;
      } else {
         int cmp = this.token.compareTo(pos.token);
         if(cmp != 0) {
            return cmp;
         } else if(pos.kind != PartitionPosition.Kind.ROW_KEY) {
            return -pos.compareTo(this);
         } else {
            DecoratedKey other = (DecoratedKey)pos;
            return this.compareKeys(other);
         }
      }
   }

   public static int compareTo(IPartitioner partitioner, ByteBuffer key, PartitionPosition position) {
      if(!(position instanceof DecoratedKey)) {
         return -position.compareTo(partitioner.decorateKey(key));
      } else {
         DecoratedKey otherKey = (DecoratedKey)position;
         int cmp = partitioner.getToken(key).compareTo(otherKey.getToken());
         return cmp == 0?ByteBufferUtil.compareUnsigned(key, otherKey.getKey()):cmp;
      }
   }

   public ByteSource asByteComparableSource() {
      return ByteSource.of(new ByteSource[]{this.token.asByteComparableSource(), ByteSource.of(this.getKey())});
   }

   public IPartitioner getPartitioner() {
      return this.token.getPartitioner();
   }

   public Token.KeyBound minValue() {
      return this.getPartitioner().getMinimumToken().minKeyBound();
   }

   public boolean isMinimum() {
      return false;
   }

   public String toString() {
      String keystring = this.getKey() == null?"null":ByteBufferUtil.bytesToHex(this.getKey());
      return "DecoratedKey(" + this.getToken() + ", " + keystring + ")";
   }

   public abstract ByteBuffer getKey();

   public void filterHash(long[] dest) {
      ByteBuffer key = this.getKey();
      MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0L, dest);
   }

   public ByteBuffer cloneKey(AbstractAllocator allocator) {
      return allocator.clone(this.getKey());
   }
}
