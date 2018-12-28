package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.ThreadsFactory;
import org.apache.cassandra.utils.time.ApolloTime;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.queues.MessagePassingQueue.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParkedThreadsMonitor {
   public static final Supplier<ParkedThreadsMonitor> instance = Suppliers.memoize(ParkedThreadsMonitor::<init>);
   private static final Logger LOGGER = LoggerFactory.getLogger(ParkedThreadsMonitor.class);
   private static final long SLEEP_INTERVAL_NS = PropertyConfiguration.getLong("dse.thread_monitor_sleep_nanos", 50000L);
   private static final boolean AUTO_CALIBRATE;
   private static final ParkedThreadsMonitor.Sleeper SLEEPER;
   private final MpscUnboundedArrayQueue<Runnable> commands = new MpscUnboundedArrayQueue(128);
   private final ArrayList<ParkedThreadsMonitor.MonitorableThread> monitoredThreads = new ArrayList(Runtime.getRuntime().availableProcessors() * 2);
   private final ArrayList<Runnable> loopActions = new ArrayList(4);
   private final Thread watcherThread = ThreadsFactory.newDaemonThread(this::run, "ParkedThreadsMonitor");
   private volatile boolean shutdown = false;

   private ParkedThreadsMonitor() {
      this.watcherThread.setPriority(10);
      this.watcherThread.start();
   }

   private void run() {
      ArrayList<Runnable> loopActions = this.loopActions;
      ArrayList<ParkedThreadsMonitor.MonitorableThread> monitoredThreads = this.monitoredThreads;

      for(MpscUnboundedArrayQueue commands = this.commands; !this.shutdown; SLEEPER.sleep()) {
         try {
            this.runCommands(commands);
            this.executeLoopActions(loopActions);
            this.monitorThreads(monitoredThreads);
         } catch (Throwable var5) {
            JVMStabilityInspector.inspectThrowable(var5);
            LOGGER.error("ParkedThreadsMonitor exception: ", var5);
         }
      }

      this.unparkOnShutdown(monitoredThreads);
   }

   private void monitorThreads(ArrayList<ParkedThreadsMonitor.MonitorableThread> monitoredThreads) {
      for(int i = 0; i < monitoredThreads.size(); ++i) {
         ParkedThreadsMonitor.MonitorableThread thread = (ParkedThreadsMonitor.MonitorableThread)monitoredThreads.get(i);

         try {
            if(thread.shouldUnpark()) {
               thread.unpark();
            }
         } catch (Throwable var5) {
            LOGGER.error("Exception unparking a monitored thread", var5);
         }
      }

   }

   private void executeLoopActions(ArrayList<Runnable> loopActions) {
      for(int i = 0; i < loopActions.size(); ++i) {
         Runnable action = (Runnable)loopActions.get(i);

         try {
            action.run();
         } catch (Throwable var5) {
            LOGGER.error("Exception running an action", var5);
         }
      }

   }

   private void runCommands(MpscUnboundedArrayQueue<Runnable> commands) {
      if(!commands.isEmpty()) {
         commands.drain(Runnable::run);
      }

   }

   private void unparkOnShutdown(ArrayList<ParkedThreadsMonitor.MonitorableThread> monitoredThreads) {
      Iterator var2 = monitoredThreads.iterator();

      while(var2.hasNext()) {
         ParkedThreadsMonitor.MonitorableThread thread = (ParkedThreadsMonitor.MonitorableThread)var2.next();
         thread.unpark();
      }

   }

   public void addThreadsToMonitor(Collection<ParkedThreadsMonitor.MonitorableThread> threads) {
      threads.forEach(this::addThreadToMonitor);
   }

   public void addThreadToMonitor(ParkedThreadsMonitor.MonitorableThread thread) {
      this.commands.offer(() -> {
         this.monitoredThreads.add(thread);
      });
   }

   public void removeThreadToMonitor(ParkedThreadsMonitor.MonitorableThread thread) {
      this.commands.offer(() -> {
         this.monitoredThreads.remove(thread);
      });
   }

   public void removeThreadsToMonitor(Collection<ParkedThreadsMonitor.MonitorableThread> threads) {
      threads.forEach(this::removeThreadToMonitor);
   }

   public void addAction(Runnable action) {
      this.commands.offer(() -> {
         this.loopActions.add(action);
      });
   }

   public void shutdown() {
      this.shutdown = true;
   }

   public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
      this.shutdown();
      this.watcherThread.join(timeUnit.toMillis(timeout));
      return !this.watcherThread.isAlive();
   }

   static {
      AUTO_CALIBRATE = PropertyConfiguration.getBoolean("dse.thread_monitor_auto_calibrate", true) && SLEEP_INTERVAL_NS > 0L;
      SLEEPER = (ParkedThreadsMonitor.Sleeper)(AUTO_CALIBRATE?new ParkedThreadsMonitor.CalibratingSleeper(SLEEP_INTERVAL_NS):new ParkedThreadsMonitor.Sleeper());
   }

   @VisibleForTesting
   public static class CalibratingSleeper extends ParkedThreadsMonitor.Sleeper {
      final long targetNs;
      long calibratedSleepNs;
      long expSmoothedSleepTimeNs;
      int comparisonDelay;

      @VisibleForTesting
      public CalibratingSleeper(long targetNs) {
         this.targetNs = targetNs;
         this.calibratedSleepNs = targetNs;
         this.expSmoothedSleepTimeNs = 0L;
      }

      @VisibleForTesting
      public void sleep() {
         long start = this.nanoTime();
         this.park();
         long sleptNs = this.nanoTime() - start;
         if(this.expSmoothedSleepTimeNs == 0L) {
            this.expSmoothedSleepTimeNs = sleptNs;
         } else {
            this.expSmoothedSleepTimeNs = (long)(0.001D * (double)sleptNs + 0.999D * (double)this.expSmoothedSleepTimeNs);
         }

         if(this.comparisonDelay < 100) {
            ++this.comparisonDelay;
         } else if((double)this.expSmoothedSleepTimeNs > (double)this.targetNs * 1.1D) {
            this.calibratedSleepNs = (long)((double)this.calibratedSleepNs * 0.9D) + 1L;
            this.expSmoothedSleepTimeNs = 0L;
            this.comparisonDelay = 0;
         } else if((double)this.expSmoothedSleepTimeNs < (double)this.targetNs * 0.9D) {
            this.calibratedSleepNs = (long)((double)this.calibratedSleepNs * 1.1D);
            this.expSmoothedSleepTimeNs = 0L;
            this.comparisonDelay = 0;
         }

      }

      @VisibleForTesting
      void park() {
         LockSupport.parkNanos(this.calibratedSleepNs);
      }

      @VisibleForTesting
      long nanoTime() {
         return ApolloTime.highPrecisionNanoTime();
      }
   }

   static class Sleeper {
      Sleeper() {
      }

      void sleep() {
         if(ParkedThreadsMonitor.SLEEP_INTERVAL_NS > 0L) {
            LockSupport.parkNanos(ParkedThreadsMonitor.SLEEP_INTERVAL_NS);
         }

      }
   }

   public interface MonitorableThread {
      void unpark();

      boolean shouldUnpark();

      public static enum ThreadState {
         PARKED,
         WORKING;

         private ThreadState() {
         }
      }
   }
}
