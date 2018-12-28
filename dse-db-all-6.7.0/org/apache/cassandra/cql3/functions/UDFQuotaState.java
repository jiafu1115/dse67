package org.apache.cassandra.cql3.functions;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import org.apache.cassandra.config.PropertyConfiguration;

final class UDFQuotaState {
   private static final ThreadMXBean threadMXBean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
   private static final int checkInterval = PropertyConfiguration.getInteger("cassandra.java_udf_check_interval", 1000);
   private final long threadId = Thread.currentThread().getId();
   final long failCpuTimeNanos;
   final long warnCpuTimeNanos;
   final long failBytes;
   final long warnBytes;
   private long cpuTimeOffset;
   private long bytesOffset;
   private int checkEvery;
   private boolean failed;

   UDFQuotaState(long failCpuTimeNanos, long warnCpuTimeNanos, long failMemoryMb, long warnMemoryMb) {
      this.failCpuTimeNanos = failCpuTimeNanos;
      this.warnCpuTimeNanos = warnCpuTimeNanos;
      this.failBytes = failMemoryMb * 1024L * 1024L;
      this.warnBytes = warnMemoryMb * 1024L * 1024L;
      this.checkEvery = checkInterval;
   }

   void onStart() {
      this.bytesOffset = threadMXBean.getThreadAllocatedBytes(this.threadId);
      this.cpuTimeOffset = threadMXBean.getThreadCpuTime(this.threadId);

      assert this.cpuTimeOffset != -1L : "ThreadMXBean does not support thread CPU time";

   }

   boolean failCheckLazy() {
      if(this.checkEvery-- <= 0) {
         if(this.cpuTimeNanos() > this.failCpuTimeNanos || this.allocatedBytes() > this.failBytes) {
            this.failed = true;
         }

         this.checkEvery = checkInterval;
      }

      return this.failed;
   }

   UDFExecResult afterExec(UDFExecResult result) {
      long threadCpuTime = result.cpuTime = this.cpuTimeNanos();
      result.failCpuTimeExceeded = threadCpuTime > this.failCpuTimeNanos;
      result.warnCpuTimeExceeded = threadCpuTime > this.warnCpuTimeNanos;
      long allocatedBytes = result.allocatedBytes = this.allocatedBytes();
      result.failAllocatedBytesExceeded = allocatedBytes > this.failBytes;
      result.warnAllocatedBytesExceeded = allocatedBytes > this.warnBytes;
      return result;
   }

   long cpuTimeNanos() {
      return threadMXBean.getThreadCpuTime(this.threadId) - this.cpuTimeOffset;
   }

   long allocatedBytes() {
      return threadMXBean.getThreadAllocatedBytes(this.threadId) - this.bytesOffset;
   }
}
