package org.apache.cassandra.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

public class CompactEndpointSerializationHelper {
   public CompactEndpointSerializationHelper() {
   }

   public static void serialize(InetAddress endpoint, DataOutput out) throws IOException {
      byte[] buf = endpoint.getAddress();
      out.writeByte(buf.length);
      out.write(buf);
   }

   public static InetAddress deserialize(DataInput in) throws IOException {
      byte[] bytes = new byte[in.readByte()];
      in.readFully(bytes, 0, bytes.length);
      return InetAddress.getByAddress(bytes);
   }

   public static int serializedSize(InetAddress from) {
      if(from instanceof Inet4Address) {
         return 5;
      } else {
         assert from instanceof Inet6Address;

         return 17;
      }
   }
}
