package org.apache.cassandra.streaming;

public class StreamException extends Exception {
   public final StreamState finalState;

   public StreamException(StreamState finalState, String message) {
      super(message);
      this.finalState = finalState;
   }

   public StreamException(StreamState finalState, String message, Throwable cause) {
      super(message, cause);
      this.finalState = finalState;
   }
}
