package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.dht.Token;

public class PreHashedDecoratedKey extends BufferDecoratedKey {
   final long hash0;
   final long hash1;

   public PreHashedDecoratedKey(Token token, ByteBuffer key, long hash0, long hash1) {
      super(token, key);
      this.hash0 = hash0;
      this.hash1 = hash1;
   }

   public void filterHash(long[] dest) {
      dest[0] = this.hash0;
      dest[1] = this.hash1;
   }
}
