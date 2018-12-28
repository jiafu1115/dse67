package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoop;
import io.netty.channel.epoll.Native;
import io.netty.channel.epoll.AIOContext.Config;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Contended;

public class EpollTPCEventLoopGroup extends MultithreadEventLoopGroup implements TPCEventLoopGroup {
   private static final String DEBUG_RUNNING_TIME_NAME = "dse.tpc.debug_task_running_time_seconds";
   private static final long DEBUG_RUNNING_TIME_NANOS;
   private static final Logger LOGGER;
   private static final boolean DISABLE_BACKPRESSURE;
   private static final int REMOTE_BACKPRESSURE_MULTIPLIER;
   private static final int GLOBAL_BACKPRESSURE_MULTIPLIER;
   private static final long SCHEDULED_CHECK_INTERVAL_NANOS;
   private static final long EPOLL_CHECK_INTERVAL_NANOS;
   private static final boolean DO_EPOLL_CHECK;
   private static final long BUSY_BACKOFF;
   private static final long YIELD_BACKOFF;
   private static final long PARK_BACKOFF;
   public static final String USE_HIGH_ALERT_PROPERTY = "dse.tpc.use_high_alert";
   private static final boolean USE_HIGH_ALERT;
   private static final boolean TPC_ONLY_HIGH_ALERT;
   private static final long HIGH_ALERT_SPIN_FACTOR;
   private static final long HIGH_ALERT_LENGTH_NS;
   private static final long SKIP_BACKOFF_STAGE = 0L;
   private static final long LAST_BACKOFF_STAGE = -1L;
   private static final int TASKS_LIMIT;
   @Contended
   private final UnmodifiableArrayList<EpollTPCEventLoopGroup.SingleCoreEventLoop> eventLoops;
   private volatile boolean shutdown;

   @VisibleForTesting
   public EpollTPCEventLoopGroup(int nThreads, String name) {
      this(nThreads);
   }

   public EpollTPCEventLoopGroup(int nThreads) {
      super(nThreads, TPCThread.newTPCThreadFactory(), new Object[0]);
      this.eventLoops = UnmodifiableArrayList.copyOf(Iterables.transform(this, (e) -> {
         return (EpollTPCEventLoopGroup.SingleCoreEventLoop)e;
      }));
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).addThreadsToMonitor(new ArrayList(this.eventLoops));
      if(!DISABLE_BACKPRESSURE) {
         LOGGER.info("Enabled TPC backpressure with {} pending requests limit, remote multiplier at {}, global multiplier at {}", new Object[]{Integer.valueOf(DatabaseDescriptor.getTPCPendingRequestsLimit()), Integer.valueOf(REMOTE_BACKPRESSURE_MULTIPLIER), Integer.valueOf(GLOBAL_BACKPRESSURE_MULTIPLIER)});
      } else {
         LOGGER.warn("TPC backpressure is disabled. NOT RECOMMENDED.");
      }

      LOGGER.info("TPC extended backoff is {}", USE_HIGH_ALERT?"enabled":"disabled");
   }

   public UnmodifiableArrayList<? extends TPCEventLoop> eventLoops() {
      return this.eventLoops;
   }

   public void shutdown() {
      super.shutdown();
      ((ParkedThreadsMonitor)ParkedThreadsMonitor.instance.get()).removeThreadsToMonitor(new ArrayList(this.eventLoops));
      this.shutdown = true;
   }

   protected EventLoop newChild(Executor executor, Object... args) throws Exception {
      assert executor instanceof TPCThread.TPCThreadsCreator;

      TPCThread.TPCThreadsCreator tpcThreadsCreator = (TPCThread.TPCThreadsCreator)executor;
      int nextCore = tpcThreadsCreator.lastCreatedThread() == null?0:tpcThreadsCreator.lastCreatedThread().coreId() + 1;
      return new EpollTPCEventLoopGroup.SingleCoreEventLoop(this, tpcThreadsCreator, TPC.aioCoordinator.getIOConfig(nextCore));
   }

   static {
      DEBUG_RUNNING_TIME_NANOS = TimeUnit.SECONDS.toNanos((long)PropertyConfiguration.getInteger("dse.tpc.debug_task_running_time_seconds", 0));
      LOGGER = LoggerFactory.getLogger(EpollTPCEventLoopGroup.class);
      DISABLE_BACKPRESSURE = PropertyConfiguration.getBoolean("dse.tpc.disable_backpressure", false);
      REMOTE_BACKPRESSURE_MULTIPLIER = PropertyConfiguration.getInteger("dse.tpc.remote_backpressure_multiplier", 5);
      GLOBAL_BACKPRESSURE_MULTIPLIER = PropertyConfiguration.getInteger("dse.tpc.global_backpressure_multiplier", Math.max(10, DatabaseDescriptor.getTPCCores()));
      SCHEDULED_CHECK_INTERVAL_NANOS = PropertyConfiguration.getLong("netty.schedule_check_interval_nanos", 1000L);
      EPOLL_CHECK_INTERVAL_NANOS = PropertyConfiguration.getLong("netty.epoll_check_interval_nanos", 2000L);
      DO_EPOLL_CHECK = EPOLL_CHECK_INTERVAL_NANOS != -1L;
      BUSY_BACKOFF = PropertyConfiguration.getLong("netty.eventloop.busy_extra_spins", 10L);
      YIELD_BACKOFF = PropertyConfiguration.getLong("netty.eventloop.yield_extra_spins", 0L);
      PARK_BACKOFF = PropertyConfiguration.getLong("netty.eventloop.park_extra_spins", 0L);
      USE_HIGH_ALERT = PropertyConfiguration.getBoolean("dse.tpc.use_high_alert", true);
      TPC_ONLY_HIGH_ALERT = PropertyConfiguration.getBoolean("dse.tpc.tpc_only_high_alert", false);
      HIGH_ALERT_SPIN_FACTOR = PropertyConfiguration.getLong("dse.tpc.high_alert_spin_factor", 100L);
      HIGH_ALERT_LENGTH_NS = PropertyConfiguration.getLong("dse.tpc.high_alert_length_ns", 750000L);
      TASKS_LIMIT = PropertyConfiguration.getInteger("netty.eventloop.tasks_processing_limit", 1024);
   }

   public static class SingleCoreEventLoop extends EpollEventLoop implements TPCEventLoop, ParkedThreadsMonitor.MonitorableThread {
      private static final boolean DEBUG_TPC_SCHEDULING;
      private static final long DEBUG_TPC_SCHEDULING_DELAY_NS;
      private static final int MAX_HIGH_ALERT;
      private volatile long drainTimeSinceStartupNs;
      private final EpollTPCEventLoopGroup parent;
      private final TPCThread thread;
      private final TPCMetrics metrics;
      private final TPCMetricsAndLimits.TaskStats busySpinStats;
      private final TPCMetricsAndLimits.TaskStats yieldStats;
      private final TPCMetricsAndLimits.TaskStats parkStats;
      private final MpscArrayQueue<Runnable> queue;
      private final MpscArrayQueue<TPCRunnable> pendingQueue;
      private final MpscArrayQueue<Runnable> priorityQueue;
      private final TPCMetricsAndLimits.TaskStats selectStats;
      private final TPCMetricsAndLimits.TaskStats selectNowStats;
      private final TPCMetricsAndLimits.TaskStats selectorEventsStats;
      private final TPCMetricsAndLimits.TaskStats scheduledTasksStats;
      private final TPCMetricsAndLimits.TaskStats processedTasksStats;
      @Contended
      private volatile ParkedThreadsMonitor.MonitorableThread.ThreadState state;
      private final CountDownLatch racyInit;
      private int pendingEpollEvents;
      private long lastEpollCheckNs;
      private long lastScheduledCheckNs;
      private boolean isHighAlertPeriod;
      private long highAlertStartNs;
      private static final AtomicInteger highAlertLimiter;
      private boolean hasGlobalBackpressure;
      private static final int LOCAL_BACKPRESSURE_THRESHOLD;
      private static final int REMOTE_BACKPRESSURE_THRESHOLD;
      private static final int GLOBAL_BACKPRESSURE_THRESHOLD;

      private SingleCoreEventLoop(EpollTPCEventLoopGroup parent, TPCThread.TPCThreadsCreator executor, Config aio) {
         super(parent, executor, 0, DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy(), RejectedExecutionHandlers.reject(), aio);
         this.racyInit = new CountDownLatch(1);
         this.pendingEpollEvents = 0;
         this.lastEpollCheckNs = timeSinceStartupNs();
         this.lastScheduledCheckNs = this.lastEpollCheckNs;
         this.highAlertStartNs = 9223372036854775807L;
         this.parent = parent;
         this.queue = new MpscArrayQueue(65536);
         this.pendingQueue = new MpscArrayQueue(65536);
         this.priorityQueue = new MpscArrayQueue(16);
         this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
         this.drainTimeSinceStartupNs = -1L;
         this.submit(() -> {
         });
         this.thread = executor.lastCreatedThread();

         assert this.thread != null;

         TPCMetricsAndLimits metrics = (TPCMetricsAndLimits)this.thread.metrics();
         this.metrics = metrics;
         this.busySpinStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SPIN);
         this.yieldStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_YIELD);
         this.parkStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_PARK);
         this.selectStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SELECT_CALLS);
         this.selectNowStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SELECT_NOW_CALLS);
         this.selectorEventsStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SELECTOR_EVENTS);
         this.scheduledTasksStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_SCHEDULED_TASKS);
         this.processedTasksStats = metrics.getTaskStats(TPCTaskType.EVENTLOOP_PROCESSED_TASKS);
         this.thread.eventLoop(this);
      }

      public void start() {
         this.racyInit.countDown();
      }

      public boolean shouldBackpressure(boolean remote) {
         if(EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE) {
            return false;
         } else if(remote && this.remoteBackpressure()) {
            NoSpamLogger.log(EpollTPCEventLoopGroup.LOGGER, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, "Remote TPC backpressure is active with count {}.", new Object[]{Long.valueOf(this.metrics.backpressureCountedRemoteTaskCount())});
            return true;
         } else if(!remote && this.localBackpressure()) {
            NoSpamLogger.log(EpollTPCEventLoopGroup.LOGGER, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, "Local TPC backpressure is active with count {}.", new Object[]{Long.valueOf(this.metrics.backpressureCountedLocalTaskCount())});
            return true;
         } else {
            int globallyBackpressuredCores = TPCMetrics.globallyBackpressuredCores();
            if(globallyBackpressuredCores > 0) {
               NoSpamLogger.log(EpollTPCEventLoopGroup.LOGGER, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, "Global TPC backpressure is active, thread count is {}.", new Object[]{Integer.valueOf(globallyBackpressuredCores)});
               return true;
            } else {
               return false;
            }
         }
      }

      private boolean localBackpressure() {
         return !EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE && this.metrics.backpressureCountedLocalTaskCount() >= (long)LOCAL_BACKPRESSURE_THRESHOLD;
      }

      private boolean remoteBackpressure() {
         return !EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE && this.metrics.backpressureCountedRemoteTaskCount() >= (long)REMOTE_BACKPRESSURE_THRESHOLD;
      }

      private boolean globalBackpressure() {
         return !EpollTPCEventLoopGroup.DISABLE_BACKPRESSURE && this.metrics.backpressureCountedTotalTaskCount() >= (long)GLOBAL_BACKPRESSURE_THRESHOLD;
      }

      public TPCThread thread() {
         return this.thread;
      }

      public TPCEventLoopGroup parent() {
         return this.parent;
      }

      public boolean wakesUpForTask(Runnable task) {
         return false;
      }

      public void unpark() {
         if(DEBUG_TPC_SCHEDULING) {
            this.logUnpark();
         }

         Native.eventFdWrite(this.eventFd.intValue(), 1L);
      }

      public boolean shouldUnpark() {
         if(EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS > 0L && this.state == ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING) {
            this.checkLongRunningTasks();
         }

         return this.state == ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED && (this.hasQueueTasks() || this.hasPendingTasks());
      }

      public boolean canExecuteImmediately(TPCTaskType taskType) {
         return this.coreId() == TPCUtils.getCoreId() && !taskType.alwaysEnqueue()?(!taskType.pendable()?true:(!this.pendingQueue.isEmpty()?false:this.queue.size() < this.metrics.maxQueueSize())):false;
      }

      protected void run() {
         Uninterruptibles.awaitUninterruptibly(this.racyInit);

         while(!this.parent.shutdown) {
            try {
               if(this.processEvents(timeSinceStartupNs()) == 0) {
                  this.waitForWork();
               }
            } catch (Throwable var2) {
               JVMStabilityInspector.inspectThrowable(var2);
               EpollTPCEventLoopGroup.LOGGER.error("Error in event loop:", var2);
            }
         }

         if(this.isShuttingDown()) {
            this.closeAll();
            this.confirmShutdown();
         }

      }

      protected boolean removeTask(Runnable task) {
         return true;
      }

      protected void addTask(Runnable task) {
         if(DEBUG_TPC_SCHEDULING) {
            this.logAddTask(task);
         }

         boolean isTpcTask = task instanceof TPCRunnable;
         if(isTpcTask) {
            TPCRunnable tpc = (TPCRunnable)task;
            if(tpc.hasPriority() && this.priorityQueue.relaxedOffer(tpc)) {
               if(EpollTPCEventLoopGroup.USE_HIGH_ALERT) {
                  this.maybeTriggerHighAlert(true);
               }

               return;
            }

            if(tpc.isPendable()) {
               if(this.pendingQueue.isEmpty() && this.queue.offerIfBelowThreshold(tpc, this.metrics.maxQueueSize())) {
                  if(EpollTPCEventLoopGroup.USE_HIGH_ALERT) {
                     this.maybeTriggerHighAlert(true);
                  }

                  return;
               }

               if(this.pendingQueue.relaxedOffer(tpc)) {
                  if(EpollTPCEventLoopGroup.USE_HIGH_ALERT) {
                     this.maybeTriggerHighAlert(true);
                  }

                  tpc.setPending();
                  return;
               }

               tpc.blocked();
               this.reject(task);
               return;
            }
         }

         if(!this.queue.relaxedOffer(task)) {
            this.reject(task);
         } else if(EpollTPCEventLoopGroup.USE_HIGH_ALERT) {
            this.maybeTriggerHighAlert(isTpcTask);
         }

      }

      private void maybeTriggerHighAlert(boolean isTpcTask) {
         if(!EpollTPCEventLoopGroup.TPC_ONLY_HIGH_ALERT || isTpcTask) {
            if(!TPCUtils.isTPCThread()) {
               return;
            }

            EpollTPCEventLoopGroup.SingleCoreEventLoop senderEventLoop = ((TPCThread)Thread.currentThread()).eventLoop();
            if(senderEventLoop == this || senderEventLoop.isHighAlertPeriod) {
               return;
            }

            int highAlertIndex = highAlertLimiter.getAndIncrement();
            if(highAlertIndex < MAX_HIGH_ALERT) {
               senderEventLoop.isHighAlertPeriod = true;
               long timeSinceStartupNs = timeSinceStartupNs();
               senderEventLoop.highAlertStartNs = timeSinceStartupNs;
               if(DEBUG_TPC_SCHEDULING) {
                  this.logHighAlertStarted(senderEventLoop, highAlertIndex, timeSinceStartupNs);
               }
            } else {
               highAlertIndex = highAlertLimiter.getAndDecrement();
               if(DEBUG_TPC_SCHEDULING) {
                  this.logHighAlertLimited(senderEventLoop, highAlertIndex);
               }
            }
         }

      }

      protected boolean hasTasks() {
         assert this.inEventLoop();

         return this.hasQueueTasks() || this.hasPendingTasks() || this.throttledHasScheduledEvents(timeSinceStartupNs());
      }

      private void waitForWork() {
         int idle = 0;
         long waitForWorkStartNs = DEBUG_TPC_SCHEDULING?timeSinceStartupNs():0L;

         boolean shouldContinue;
         do {
            boolean var10000;
            if(EpollTPCEventLoopGroup.USE_HIGH_ALERT && this.isHighAlertPeriod) {
               ++idle;
               var10000 = this.highAlertBackoff(idle, waitForWorkStartNs);
            } else {
               ++idle;
               var10000 = this.backoff(idle);
            }

            shouldContinue = var10000;
         } while(!this.parent.shutdown && shouldContinue && this.isIdle(idle));

         if(DEBUG_TPC_SCHEDULING) {
            this.logWaitEnded(waitForWorkStartNs);
         }

         if(EpollTPCEventLoopGroup.USE_HIGH_ALERT) {
            this.checkIfHighAlertEnded(timeSinceStartupNs());
         }

      }

      private boolean isIdle(int idle) {
         if(!this.hasQueueTasks() && !this.hasPendingTasks()) {
            if((long)idle <= EpollTPCEventLoopGroup.BUSY_BACKOFF - 1L && idle % 1024 != 1023) {
               return true;
            } else {
               long timeSinceStartupNs = timeSinceStartupNs();
               return !this.throttledHasScheduledEvents(timeSinceStartupNs) && !this.throttledHasEpollEvents(timeSinceStartupNs);
            }
         } else {
            return false;
         }
      }

      private void checkIfHighAlertEnded(long timeSinceStartupNs) {
         long alertPeriodLength = this.isHighAlertPeriod?timeSinceStartupNs - this.highAlertStartNs:0L;
         if(this.isHighAlertPeriod && alertPeriodLength > EpollTPCEventLoopGroup.HIGH_ALERT_LENGTH_NS) {
            this.isHighAlertPeriod = false;
            this.highAlertStartNs = 9223372036854775807L;
            int highAlertIndex = highAlertLimiter.decrementAndGet();
            if(DEBUG_TPC_SCHEDULING) {
               this.logHighAlertEnd(timeSinceStartupNs, alertPeriodLength, highAlertIndex);
            }
         }

      }

      private boolean throttledHasScheduledEvents(long timeSinceStartupNs) {
         if(timeSinceStartupNs - this.lastScheduledCheckNs > EpollTPCEventLoopGroup.SCHEDULED_CHECK_INTERVAL_NANOS) {
            boolean result = this.hasScheduledTasks(timeSinceStartupNs);
            if(!result) {
               this.lastScheduledCheckNs = timeSinceStartupNs;
            }

            return result;
         } else {
            return false;
         }
      }

      private boolean throttledHasEpollEvents(long timeSinceStartupNs) {
         if(EpollTPCEventLoopGroup.DO_EPOLL_CHECK && timeSinceStartupNs - this.lastEpollCheckNs > EpollTPCEventLoopGroup.EPOLL_CHECK_INTERVAL_NANOS && this.pendingEpollEvents == 0) {
            this.epollSelectNow(timeSinceStartupNs);
         }

         return this.pendingEpollEvents != 0;
      }

      private boolean backoff(int idle) {
         if(EpollTPCEventLoopGroup.BUSY_BACKOFF == -1L || EpollTPCEventLoopGroup.BUSY_BACKOFF != 0L && (long)idle < EpollTPCEventLoopGroup.BUSY_BACKOFF) {
            this.busy();
         } else if(EpollTPCEventLoopGroup.YIELD_BACKOFF == -1L || EpollTPCEventLoopGroup.YIELD_BACKOFF != 0L && (long)idle < EpollTPCEventLoopGroup.BUSY_BACKOFF + EpollTPCEventLoopGroup.YIELD_BACKOFF) {
            this.yield();
         } else {
            if(EpollTPCEventLoopGroup.PARK_BACKOFF != -1L && (EpollTPCEventLoopGroup.PARK_BACKOFF == 0L || (long)idle >= EpollTPCEventLoopGroup.BUSY_BACKOFF + EpollTPCEventLoopGroup.YIELD_BACKOFF + EpollTPCEventLoopGroup.PARK_BACKOFF)) {
               this.parkOnEpollWait();
               return false;
            }

            this.park();
         }

         return true;
      }

      private boolean highAlertBackoff(int idle, long waitForWorkStartNs) {
         if(EpollTPCEventLoopGroup.BUSY_BACKOFF != -1L && (long)idle >= EpollTPCEventLoopGroup.BUSY_BACKOFF) {
            if(EpollTPCEventLoopGroup.YIELD_BACKOFF != -1L && (long)idle >= EpollTPCEventLoopGroup.BUSY_BACKOFF + EpollTPCEventLoopGroup.YIELD_BACKOFF + EpollTPCEventLoopGroup.HIGH_ALERT_SPIN_FACTOR) {
               if(EpollTPCEventLoopGroup.PARK_BACKOFF != -1L && (double)idle >= (double)EpollTPCEventLoopGroup.BUSY_BACKOFF + (double)(EpollTPCEventLoopGroup.YIELD_BACKOFF + EpollTPCEventLoopGroup.PARK_BACKOFF) + (double)EpollTPCEventLoopGroup.HIGH_ALERT_SPIN_FACTOR * 1.1D) {
                  if(DEBUG_TPC_SCHEDULING) {
                     this.logTimeToPark(waitForWorkStartNs);
                  }

                  this.parkOnEpollWait();
                  return false;
               }

               this.park();
            } else {
               this.yield();
            }
         } else {
            this.busy();
         }

         return true;
      }

      private void park() {
         this.parkStats.scheduledTasks.add(1L);
         this.parkStats.completedTasks.add(1L);
         LockSupport.parkNanos(1L);
      }

      private void yield() {
         this.yieldStats.scheduledTasks.add(1L);
         this.yieldStats.completedTasks.add(1L);
         Thread.yield();
      }

      private void busy() {
         this.busySpinStats.scheduledTasks.add(1L);
         this.busySpinStats.completedTasks.add(1L);
      }

      private void parkOnEpollWait() {
         this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.PARKED;
         this.epollSelect();
         if(EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS > 0L) {
            this.drainTimeSinceStartupNs = timeSinceStartupNs();
         }

         this.state = ParkedThreadsMonitor.MonitorableThread.ThreadState.WORKING;
      }

      private void checkLongRunningTasks() {
         if(this.drainTimeSinceStartupNs > 0L) {
            long timeSinceStartupNs = timeSinceStartupNs();
            long diff = Math.abs(timeSinceStartupNs - this.drainTimeSinceStartupNs);
            if(diff > EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS) {
               if(EpollTPCEventLoopGroup.LOGGER.isDebugEnabled()) {
                  EpollTPCEventLoopGroup.LOGGER.debug("Detected task running for {} seconds for thread with stack:\n{}", Long.valueOf(TimeUnit.SECONDS.convert(diff, TimeUnit.NANOSECONDS)), FBUtilities.Debug.getStackTrace((Thread)this.thread));
               }

               this.drainTimeSinceStartupNs = timeSinceStartupNs;
            }

         }
      }

      private int processEvents(long timeSinceStartupNs) {
         if(EpollTPCEventLoopGroup.DEBUG_RUNNING_TIME_NANOS > 0L) {
            this.drainTimeSinceStartupNs = timeSinceStartupNs;
         }

         int scheduled = 0;
         int epoll = 0;
         if(this.throttledHasEpollEvents(timeSinceStartupNs)) {
            epoll = this.processEpollEvents();
         }

         if(timeSinceStartupNs - this.lastScheduledCheckNs > EpollTPCEventLoopGroup.SCHEDULED_CHECK_INTERVAL_NANOS) {
            scheduled = this.runScheduledTasks(timeSinceStartupNs);
         }

         int tasks = this.processTasks();
         int transfered = this.transferFromPendingTasks();
         if(DEBUG_TPC_SCHEDULING) {
            this.logProcessedStats(timeSinceStartupNs, epoll, scheduled, tasks, transfered);
         }

         int processed = epoll + scheduled + tasks + transfered;
         this.processedTasksStats.scheduledTasks.add((long)processed);
         this.processedTasksStats.completedTasks.add((long)processed);
         return processed;
      }

      private int processEpollEvents() {
         int currPendingEpollEvents = this.pendingEpollEvents;
         if(currPendingEpollEvents == 0) {
            return 0;
         } else {
            this.pendingEpollEvents = 0;
            this.selectorEventsStats.scheduledTasks.add((long)currPendingEpollEvents);

            byte var3;
            try {
               this.processReady(this.events, currPendingEpollEvents);
               if(this.allowGrowing && currPendingEpollEvents == this.events.length()) {
                  this.events.increase();
               }

               int var2 = currPendingEpollEvents;
               return var2;
            } catch (Throwable var7) {
               this.handleEpollEventError(var7);
               var3 = 0;
            } finally {
               this.selectorEventsStats.completedTasks.add((long)currPendingEpollEvents);
            }

            return var3;
         }
      }

      private void epollSelect() {
         if(this.pendingEpollEvents != 0) {
            throw new IllegalStateException("Should not be doing a blocking select with pendingEpollEvents=" + this.pendingEpollEvents);
         } else {
            this.selectStats.scheduledTasks.add(1L);

            try {
               this.pendingEpollEvents = this.epollWait(false);
               this.lastEpollCheckNs = timeSinceStartupNs();

               assert this.pendingEpollEvents >= 0;
            } catch (Exception var5) {
               EpollTPCEventLoopGroup.LOGGER.error("Error selecting socket ", var5);
            } finally {
               this.selectStats.completedTasks.add(1L);
            }

         }
      }

      private void epollSelectNow(long timeSinceStartupNs) {
         if(this.pendingEpollEvents != 0) {
            throw new IllegalStateException("Should not be doing a selectNow with pendingEpollEvents=" + this.pendingEpollEvents);
         } else {
            this.lastEpollCheckNs = timeSinceStartupNs;
            this.selectNowStats.scheduledTasks.add(1L);

            try {
               this.pendingEpollEvents = this.selectNowSupplier.get();

               assert this.pendingEpollEvents >= 0;
            } catch (Exception var7) {
               EpollTPCEventLoopGroup.LOGGER.error("Error selecting socket ", var7);
            } finally {
               this.selectNowStats.completedTasks.add(1L);
            }

         }
      }

      private void handleEpollEventError(Throwable e) {
         EpollTPCEventLoopGroup.LOGGER.error("Unexpected exception in the selector loop.", e);
         Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
      }

      private int processTasks() {
         MpscArrayQueue<Runnable> queue = this.queue;
         return this.processTasks(() -> {
            return (Runnable)queue.relaxedPoll();
         });
      }

      private int processTasks(Supplier<Runnable> taskSupplier) {
         int processed = 0;

         try {
            MpscArrayQueue priorityQueue = this.priorityQueue;

            Runnable pTask;
            Runnable tTask;
            do {
               pTask = (Runnable)priorityQueue.relaxedPoll();
               processed += this.process(pTask);
               tTask = (Runnable)taskSupplier.get();
               processed += this.process(tTask);
            } while(processed < EpollTPCEventLoopGroup.TASKS_LIMIT - 1 && (pTask != null || tTask != null));
         } catch (Throwable var6) {
            this.handleTaskException(var6);
         }

         return processed;
      }

      private int process(Runnable task) {
         if(task != null) {
            if(task instanceof TPCRunnable) {
               ((TPCRunnable)task).run();
            } else {
               task.run();
            }

            return 1;
         } else {
            return 0;
         }
      }

      private void handleTaskException(Throwable t) {
         JVMStabilityInspector.inspectThrowable(t);
         EpollTPCEventLoopGroup.LOGGER.error("Task exception encountered: ", t);

         try {
            RxJavaPlugins.getErrorHandler().accept(t);
         } catch (Exception var3) {
            throw new RuntimeException(var3);
         }
      }

      private int transferFromPendingTasks() {
         try {
            int processed = 0;
            int maxQueueSize = this.metrics.maxQueueSize();
            MpscArrayQueue pendingQueue = this.pendingQueue;

            TPCRunnable tpc;
            while((tpc = (TPCRunnable)pendingQueue.relaxedPeek()) != null && this.queue.offerIfBelowThreshold(tpc, maxQueueSize)) {
               ++processed;
               pendingQueue.relaxedPoll();
               tpc.unsetPending();
            }

            int var5 = processed;
            return var5;
         } finally {
            this.checkGlobalBackpressure();
         }
      }

      private int runScheduledTasks(long timeSinceStartupNs) {
         this.lastScheduledCheckNs = timeSinceStartupNs;
         return this.processTasks(() -> {
            this.scheduledTasksStats.scheduledTasks.increment();
            this.scheduledTasksStats.completedTasks.increment();
            return this.pollScheduledTask(timeSinceStartupNs);
         });
      }

      private void checkGlobalBackpressure() {
         if(this.metrics != null) {
            boolean hadGlobalBackpressure = this.hasGlobalBackpressure;
            this.hasGlobalBackpressure = this.globalBackpressure();
            if(hadGlobalBackpressure != this.hasGlobalBackpressure) {
               TPCMetrics.globallyBackpressuredCores(this.hasGlobalBackpressure?1:-1);
            }
         }

      }

      private boolean hasPendingTasks() {
         return this.pendingQueue.relaxedPeek() != null;
      }

      private boolean hasQueueTasks() {
         return this.queue.relaxedPeek() != null || this.priorityQueue.relaxedPeek() != null;
      }

      private static long timeSinceStartupNs() {
         return TPC.nanoTimeSinceStartup();
      }

      private void logAddTask(Runnable task) {
         long timeSinceStartupNs;
         if((timeSinceStartupNs = timeSinceStartupNs()) > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            EpollTPCEventLoopGroup.LOGGER.debug(timeSinceStartupNs + " : " + Thread.currentThread() + "-task->" + this.thread + ":" + task);
         }

      }

      private void logHighAlertStarted(EpollTPCEventLoopGroup.SingleCoreEventLoop senderEventLoop, int highAlertIndex, long timeSinceStartupNs) {
         if(timeSinceStartupNs > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            EpollTPCEventLoopGroup.LOGGER.debug(timeSinceStartupNs + " : " + senderEventLoop.thread + " on high alert highAlertIndex=" + highAlertIndex);
         }

      }

      private void logHighAlertLimited(EpollTPCEventLoopGroup.SingleCoreEventLoop senderEventLoop, int highAlertIndex) {
         long timeSinceStartupNs;
         if((timeSinceStartupNs = timeSinceStartupNs()) > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            EpollTPCEventLoopGroup.LOGGER.debug(timeSinceStartupNs + " : " + senderEventLoop.thread + " high alert limited highAlertIndex=" + highAlertIndex);
         }

      }

      private void logHighAlertEnd(long timeSinceStartupNs, long alertPeriodLength, int highAlertIndex) {
         if(timeSinceStartupNs > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            EpollTPCEventLoopGroup.LOGGER.debug(timeSinceStartupNs + " : " + Thread.currentThread() + " high alert ended after:" + alertPeriodLength + " highAlertIndex=" + highAlertIndex);
         }

      }

      private void logProcessedStats(long nanoTimeSinceStartup, int epoll, int scheduled, int tasks, int transfered) {
         long end;
         if((end = timeSinceStartupNs()) > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            EpollTPCEventLoopGroup.LOGGER.debug(nanoTimeSinceStartup + " : " + Thread.currentThread() + " processed (" + (end - nanoTimeSinceStartup) / 1000L + "us): tasks=" + tasks + ", scheduled=" + scheduled + ", epoll=" + epoll + ", transfered=" + transfered);
         }

      }

      private void logTimeToPark(long waitForWorkStartNs) {
         long nanoTimeSinceStartup;
         if((nanoTimeSinceStartup = timeSinceStartupNs()) > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            long backoffLength = nanoTimeSinceStartup - waitForWorkStartNs;
            EpollTPCEventLoopGroup.LOGGER.debug(nanoTimeSinceStartup + " : " + Thread.currentThread() + " extended backoff length=" + backoffLength + " ns, ended up parking");
         }

      }

      private void logUnpark() {
         long timeSinceStartupNs;
         if((timeSinceStartupNs = timeSinceStartupNs()) > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            EpollTPCEventLoopGroup.LOGGER.debug(timeSinceStartupNs + " : " + Thread.currentThread() + "-unpark->" + this.thread);
         }

      }

      private void logWaitEnded(long waitForWorkStartNs) {
         long timeSinceStartupNs;
         if((timeSinceStartupNs = timeSinceStartupNs()) > DEBUG_TPC_SCHEDULING_DELAY_NS) {
            long alertPeriodLength = this.isHighAlertPeriod?timeSinceStartupNs - this.highAlertStartNs:0L;
            EpollTPCEventLoopGroup.LOGGER.debug(timeSinceStartupNs + " : " + Thread.currentThread() + "- waited for " + (timeSinceStartupNs - waitForWorkStartNs) / 1000L + "us {isHighAlertPeriod=" + this.isHighAlertPeriod + ", alertPeriodLength=" + alertPeriodLength + ", pendingEpollEvents=" + this.pendingEpollEvents + ", hasQueueTasks=" + this.hasQueueTasks() + ", hasScheduledTasks=" + this.hasScheduledTasks() + "}");
         }

      }

      static {
         DEBUG_TPC_SCHEDULING = PropertyConfiguration.getBoolean("dse.tpc.debug.log_scheduling") && EpollTPCEventLoopGroup.LOGGER.isDebugEnabled();
         DEBUG_TPC_SCHEDULING_DELAY_NS = TimeUnit.SECONDS.toNanos(30L);
         MAX_HIGH_ALERT = Runtime.getRuntime().availableProcessors() / 2;
         highAlertLimiter = new AtomicInteger();
         LOCAL_BACKPRESSURE_THRESHOLD = DatabaseDescriptor.getTPCPendingRequestsLimit();
         REMOTE_BACKPRESSURE_THRESHOLD = LOCAL_BACKPRESSURE_THRESHOLD * EpollTPCEventLoopGroup.REMOTE_BACKPRESSURE_MULTIPLIER;
         GLOBAL_BACKPRESSURE_THRESHOLD = LOCAL_BACKPRESSURE_THRESHOLD * EpollTPCEventLoopGroup.GLOBAL_BACKPRESSURE_MULTIPLIER;
      }
   }
}
