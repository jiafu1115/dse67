package org.apache.cassandra.metrics;

import com.codahale.metrics.Clock;
import org.apache.cassandra.utils.time.ApolloTime;
import org.apache.cassandra.utils.time.ApproximateTime;

class ApproximateClock extends Clock {
   private static final Clock DEFAULT = new ApproximateClock();

   ApproximateClock() {
   }

   public static Clock defaultClock() {
      return DEFAULT;
   }

   public long getTime() {
      return ApproximateTime.systemClockMillis();
   }

   public long getTick() {
      return ApolloTime.approximateNanoTime();
   }
}
