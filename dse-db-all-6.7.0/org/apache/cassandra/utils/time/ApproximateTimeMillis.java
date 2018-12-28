package org.apache.cassandra.utils.time;

abstract class ApproximateTimeMillis extends ApproximateTimePad1 {
   volatile long millis;
   volatile long currentTimeMillis;
   long lastUpdateNs;

   ApproximateTimeMillis() {
      this.millis = ApproximateTime.START_TIME_MS;
      this.currentTimeMillis = ApproximateTime.START_TIME_MS;
   }
}
