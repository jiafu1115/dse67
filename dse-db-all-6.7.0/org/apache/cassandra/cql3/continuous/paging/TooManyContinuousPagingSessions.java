package org.apache.cassandra.cql3.continuous.paging;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class TooManyContinuousPagingSessions extends InvalidRequestException {
   public TooManyContinuousPagingSessions(long numSessions) {
      super(String.format("Invalid request, too many continuous paging sessions are already running: %d", new Object[]{Long.valueOf(numSessions)}));
   }
}
