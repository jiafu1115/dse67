package org.apache.cassandra.utils.time;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ParkedThreadsMonitor;

public class ApproximateTime extends ApproximateTimePad2 {
   static final long START_TIME_MS = System.currentTimeMillis();
   static final long START_TIME_NS = System.nanoTime();
   static final long NETTY_COMPATIBLE_START_TIME_NS = getStartTime();
   static final long NANOS_PER_MS;
   private static final ApproximateTime INSTANCE;
   private static final long ACCURACY_MICROS = 200L;

   static long getStartTime() throws RuntimeException {
      try {
         Field field = Class.forName("io.netty.util.concurrent.ScheduledFutureTask", true, ClassLoader.getSystemClassLoader()).getDeclaredField("START_TIME");
         field.setAccessible(true);
         return field.getLong(null);
      } catch (Exception var1) {
         throw new RuntimeException(var1);
      }
   }

   private ApproximateTime() {
      tick(this);
   }

   public static void tick() {
      ApproximateTime instance = INSTANCE;
      tick(instance);
   }

   private static void tick(ApproximateTime instance) {
      long now = System.nanoTime();
      instance.nanotime = now;
      if(now - instance.lastUpdateNs > NANOS_PER_MS) {
         instance.lastUpdateNs = now;
         instance.millis = START_TIME_MS + (now - START_TIME_NS) / NANOS_PER_MS;
         instance.currentTimeMillis = System.currentTimeMillis();
      }

   }

   public static long millisTime() {
      return INSTANCE.millis;
   }

   public static long systemClockMillis() {
      return INSTANCE.currentTimeMillis;
   }

   public static long millisSinceStartup() {
      return INSTANCE.millis - START_TIME_MS;
   }

   public static long nanoTime() {
      return INSTANCE.nanotime;
   }

   public static long nanoSinceStartup() {
      return INSTANCE.nanotime - NETTY_COMPATIBLE_START_TIME_NS;
   }

   public static long accuracy() {
      return TimeUnit.MILLISECONDS.convert(200L, TimeUnit.MICROSECONDS);
   }

   static {
      NANOS_PER_MS = TimeUnit.MILLISECONDS.toNanos(1L);
      INSTANCE = new ApproximateTime();
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).addAction(ApproximateTime::tick);
   }
}
