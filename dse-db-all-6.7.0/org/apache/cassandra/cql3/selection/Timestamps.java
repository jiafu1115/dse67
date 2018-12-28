package org.apache.cassandra.cql3.selection;

import com.google.common.collect.Range;
import java.nio.ByteBuffer;
import org.apache.cassandra.transport.ProtocolVersion;

interface Timestamps {
   Timestamps NO_TIMESTAMP = new Timestamps() {
      public Timestamps get(int index) {
         return this;
      }

      public Timestamps slice(Range<Integer> range) {
         return this;
      }

      public int size() {
         return 0;
      }

      public ByteBuffer toByteBuffer(ProtocolVersion protocolVersion) {
         return null;
      }

      public String toString() {
         return "no timestamp";
      }
   };

   Timestamps get(int var1);

   Timestamps slice(Range<Integer> var1);

   int size();

   ByteBuffer toByteBuffer(ProtocolVersion var1);
}
