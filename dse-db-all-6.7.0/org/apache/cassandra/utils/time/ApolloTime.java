package org.apache.cassandra.utils.time;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;

public class ApolloTime {
   private static final boolean useApproximateMilliesTime = !PropertyConfiguration.getBoolean("dse.use.system.time.millies", DatabaseDescriptor.isClientInitialized());
   private static final boolean useApproximateNanoTime = !PropertyConfiguration.getBoolean("dse.use.system.time.nanos", DatabaseDescriptor.isClientInitialized());

   public ApolloTime() {
   }

   public static long systemClockMillis() {
      return useApproximateMilliesTime?ApproximateTime.systemClockMillis():System.currentTimeMillis();
   }

   public static long highPrecisionNanoTime() {
      return System.nanoTime();
   }

   public static long approximateNanoTime() {
      return useApproximateNanoTime?ApproximateTime.nanoTime():System.nanoTime();
   }

   public static long millisTime() {
      return useApproximateMilliesTime?ApproximateTime.millisTime():ApproximateTime.START_TIME_MS + (System.nanoTime() - ApproximateTime.START_TIME_NS) / ApproximateTime.NANOS_PER_MS;
   }

   public static long millisSinceStartup() {
      return useApproximateMilliesTime?ApproximateTime.millisSinceStartup():(System.nanoTime() - ApproximateTime.START_TIME_NS) / ApproximateTime.NANOS_PER_MS;
   }

   public static long millisSinceStartupDelta(long start) {
      return Math.max(0L, millisSinceStartup() - start);
   }

   public static long nanoSinceStartup() {
      return useApproximateNanoTime?ApproximateTime.nanoSinceStartup():System.nanoTime() - ApproximateTime.START_TIME_NS;
   }

   public static long nanoSinceStartupDelta(long start) {
      return Math.max(0L, nanoSinceStartup() - start);
   }

   public static long systemClockSeconds() {
      return TimeUnit.MILLISECONDS.toSeconds(systemClockMillis());
   }

   public static int systemClockSecondsAsInt() {
      long secsLong = TimeUnit.MILLISECONDS.toSeconds(systemClockMillis());
      int result = (int)secsLong;
      if((long)result != secsLong) {
         throw new IllegalStateException("Current time in seconds out of range: " + secsLong);
      } else {
         return result;
      }
   }

   public static long systemClockMicros() {
      return TimeUnit.MILLISECONDS.toMicros(systemClockMillis());
   }
}
