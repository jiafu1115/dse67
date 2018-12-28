package com.datastax.bdp.insights.util;

import com.datastax.bdp.plugin.ThreadPoolPlugin;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTask {
   private final Logger logger = LoggerFactory.getLogger(this.getClass());
   private final String name;
   private final ScheduledFuture<?> task;
   private final CountDownLatch signal;
   private volatile long executionCount = 0L;
   private volatile long failureCount = 0L;

   public ScheduledTask(String name, ThreadPoolPlugin service, Runnable task, Duration initialDelay, Duration period) {
      this.name = name;
      this.signal = new CountDownLatch(1);
      this.task = service.scheduleAtFixedRate(() -> {
         try {
            task.run();
         } catch (Throwable var6) {
            ++this.failureCount;
            this.logger.error("task execution with name = {} failed [executionCount = {}, failureCount = {}]", new Object[]{this.name, Long.valueOf(this.executionCount), Long.valueOf(this.failureCount), var6});
         } finally {
            ++this.executionCount;
            this.signal.countDown();
         }

      }, initialDelay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
   }

   public String getName() {
      return this.name;
   }

   public long getExecutionCount() {
      return this.executionCount;
   }

   public long getFailureCount() {
      return this.failureCount;
   }

   public void awaitFirstExecution() {
      while(true) {
         try {
            if(this.executionCount == 0L) {
               this.signal.await(100L, TimeUnit.MILLISECONDS);
               continue;
            }
         } catch (InterruptedException var2) {
            this.logger.debug("InterruptedException thrown in task with name = {} during awaitFirstExecution", this.name);
         }

         return;
      }
   }

   public boolean isScheduleRunning() {
      this.logger.trace("isDone = {}, isCancelled = {}", Boolean.valueOf(this.task.isDone()), Boolean.valueOf(this.task.isCancelled()));
      return !this.task.isDone() && !this.task.isCancelled();
   }

   public boolean cancel() {
      boolean result = this.task.cancel(true);
      this.logger.trace("result of cancel is: {}", Boolean.valueOf(result));
      return result;
   }
}
