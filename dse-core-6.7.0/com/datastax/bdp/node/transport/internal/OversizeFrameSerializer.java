package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OversizeFrameSerializer implements MessageBodySerializer<OversizeFrameException> {
   public OversizeFrameSerializer() {
   }

   public void serialize(OversizeFrameException body, OutputStream stream) throws IOException {
      DataOutputStream out = new DataOutputStream(stream);
      out.writeUTF(body.getMessage() != null?body.getMessage():"");
   }

   public OversizeFrameException deserialize(InputStream stream) throws IOException {
      DataInputStream in = new DataInputStream(stream);
      String errorMessage = in.readUTF();
      return new OversizeFrameException(errorMessage);
   }
}
