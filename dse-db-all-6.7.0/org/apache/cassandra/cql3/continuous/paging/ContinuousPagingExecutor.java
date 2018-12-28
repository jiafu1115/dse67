package org.apache.cassandra.cql3.continuous.paging;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.selection.ResultBuilder;

public interface ContinuousPagingExecutor {
   long scheduleStartTimeInMillis();

   long queryStartTimeInNanos();

   ByteBuffer state(boolean var1);

   default void execute(Runnable runnable) {
      this.execute(runnable, 0L, TimeUnit.NANOSECONDS);
   }

   void execute(Runnable var1, long var2, TimeUnit var4);

   void schedule(ByteBuffer var1, ResultBuilder var2);
}
