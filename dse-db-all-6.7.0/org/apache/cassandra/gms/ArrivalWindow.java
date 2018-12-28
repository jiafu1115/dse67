package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.PropertyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ArrivalWindow {
   private static final Logger logger = LoggerFactory.getLogger(ArrivalWindow.class);
   private long tLast = 0L;
   private final ArrayBackedBoundedStats arrivalIntervals;
   private double lastReportedPhi = 4.9E-324D;
   private static final long MAX_INTERVAL_IN_NANO;

   ArrivalWindow(int size) {
      this.arrivalIntervals = new ArrayBackedBoundedStats(size);
   }

   synchronized void add(long value, InetAddress ep) {
      assert this.tLast >= 0L;

      if(this.tLast > 0L) {
         long interArrivalTime = value - this.tLast;
         if(interArrivalTime <= MAX_INTERVAL_IN_NANO) {
            this.arrivalIntervals.add(interArrivalTime);
            logger.trace("Reporting interval time of {} for {}", Long.valueOf(interArrivalTime), ep);
         } else {
            logger.trace("Ignoring interval time of {} for {}", Long.valueOf(interArrivalTime), ep);
         }
      } else {
         this.arrivalIntervals.add(FailureDetector.INITIAL_VALUE_NANOS);
      }

      this.tLast = value;
   }

   double mean() {
      return this.arrivalIntervals.mean();
   }

   double phi(long tnow) {
      assert this.arrivalIntervals.mean() > 0.0D && this.tLast > 0L;

      long t = tnow - this.tLast;
      this.lastReportedPhi = (double)t / this.mean();
      return this.lastReportedPhi;
   }

   double getLastReportedPhi() {
      return this.lastReportedPhi;
   }

   public String toString() {
      return Arrays.toString(this.arrivalIntervals.getArrivalIntervals());
   }

   static {
      MAX_INTERVAL_IN_NANO = TimeUnit.MILLISECONDS.toNanos(PropertyConfiguration.getLong("cassandra.fd_max_interval_ms", FailureDetector.INITIAL_VALUE_MS));
   }
}
