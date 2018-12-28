package org.apache.cassandra.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.time.ApolloTime;

public class SystemTimeSource implements TimeSource {
   public SystemTimeSource() {
   }

   public long currentTimeMillis() {
      return ApolloTime.systemClockMillis();
   }

   public long nanoTime() {
      return ApolloTime.highPrecisionNanoTime();
   }

   public TimeSource sleepUninterruptibly(long sleepFor, TimeUnit unit) {
      Uninterruptibles.sleepUninterruptibly(sleepFor, unit);
      return this;
   }

   public TimeSource sleep(long sleepFor, TimeUnit unit) throws InterruptedException {
      TimeUnit.NANOSECONDS.sleep(TimeUnit.NANOSECONDS.convert(sleepFor, unit));
      return this;
   }
}
