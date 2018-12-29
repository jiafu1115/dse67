package com.datastax.bdp.node.transport.internal;

import com.datastax.bdp.node.transport.MessageType;

public class SystemMessageTypes {
   public static final MessageType HANDSHAKE;
   public static final MessageType UNSUPPORTED_MESSAGE;
   public static final MessageType FAILED_PROCESSOR;
   public static final MessageType OVERSIZE_FRAME;

   private SystemMessageTypes() {
   }

   static {
      HANDSHAKE = MessageType.of(MessageType.Domain.SYSTEM, (byte)1);
      UNSUPPORTED_MESSAGE = MessageType.of(MessageType.Domain.SYSTEM,(byte) 2);
      FAILED_PROCESSOR = MessageType.of(MessageType.Domain.SYSTEM, (byte)3);
      OVERSIZE_FRAME = MessageType.of(MessageType.Domain.SYSTEM, (byte)4);
   }
}
