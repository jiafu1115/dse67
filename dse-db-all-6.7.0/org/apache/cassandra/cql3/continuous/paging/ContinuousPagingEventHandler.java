package org.apache.cassandra.cql3.continuous.paging;

import javax.annotation.Nullable;

public interface ContinuousPagingEventHandler {
   void sessionCreated(@Nullable Throwable var1);

   void sessionExecuting(long var1);

   void sessionCompleted(long var1, Throwable var3);

   void sessionRemoved();

   void sessionPaused();

   void sessionResumed(long var1);
}
