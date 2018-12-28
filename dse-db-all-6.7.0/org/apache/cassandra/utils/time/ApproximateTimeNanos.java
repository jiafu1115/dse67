package org.apache.cassandra.utils.time;

abstract class ApproximateTimeNanos extends ApproximateTimePad0 {
   volatile long nanotime;

   ApproximateTimeNanos() {
      this.nanotime = ApproximateTime.NETTY_COMPATIBLE_START_TIME_NS;
   }
}
