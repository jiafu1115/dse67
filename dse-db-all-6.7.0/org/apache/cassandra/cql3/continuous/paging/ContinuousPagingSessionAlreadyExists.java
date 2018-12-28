package org.apache.cassandra.cql3.continuous.paging;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class ContinuousPagingSessionAlreadyExists extends InvalidRequestException {
   public ContinuousPagingSessionAlreadyExists(String key) {
      super(String.format("Invalid request, already executing continuous paging session %s", new Object[]{key}));
   }
}
