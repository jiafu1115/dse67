package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class AuthSuccess extends Message.Response {
   public static final Message.Codec<AuthSuccess> codec = new Message.Codec<AuthSuccess>() {
      public AuthSuccess decode(ByteBuf body, ProtocolVersion version) {
         ByteBuffer b = CBUtil.readValue(body);
         byte[] token = null;
         if(b != null) {
            token = new byte[b.remaining()];
            b.get(token);
         }

         return new AuthSuccess(token);
      }

      public void encode(AuthSuccess success, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeValue(success.token, dest);
      }

      public int encodedSize(AuthSuccess success, ProtocolVersion version) {
         return CBUtil.sizeOfValue(success.token);
      }
   };
   private byte[] token;

   public AuthSuccess(byte[] token) {
      super(Message.Type.AUTH_SUCCESS);
      this.token = token;
   }

   public byte[] getToken() {
      return this.token;
   }
}
