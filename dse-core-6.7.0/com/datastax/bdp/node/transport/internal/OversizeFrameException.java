package com.datastax.bdp.node.transport.internal;

public class OversizeFrameException extends Exception {
   public OversizeFrameException(String errorMessage) {
      super(errorMessage);
   }
}
