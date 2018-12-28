package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.dht.Token;

public class BufferDecoratedKey extends DecoratedKey {
   private final ByteBuffer key;

   public BufferDecoratedKey(Token token, ByteBuffer key) {
      super(token);

      assert key != null;

      this.key = key;
   }

   public ByteBuffer getKey() {
      return this.key;
   }
}
