package org.apache.cassandra.concurrent;

import io.netty.channel.epoll.Aio;
import io.netty.channel.epoll.Epoll;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.AioCoordinator;
import org.apache.cassandra.metrics.TPCAggregatedStageMetrics;
import org.apache.cassandra.metrics.TPCTotalMetrics;
import org.apache.cassandra.rx.RxSubscriptionDebugger;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.CoordinatedAction;
import org.apache.cassandra.utils.concurrent.ExecutableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPC {
   private static final Logger logger = LoggerFactory.getLogger(TPC.class);
   private static final boolean LOG_CALLER_STACK_ON_EXCEPTION = PropertyConfiguration.getBoolean("dse.tpc.log_caller_stack_on_exception", false, "Set this to true in order to log the caller's thread stack trace in case of exception when running a task on an Rx scheduler.");
   private static final boolean ENABLE_RX_SUBSCRIPTION_DEBUG = PropertyConfiguration.getBoolean("dse.tpc.enable_rx_subscription_debug", false);
   public static final boolean DEBUG_DONT_SCHEDULE_METRICS = PropertyConfiguration.getBoolean("dse.tpc.debug.dont_schedule_metrics");
   private static final int TIMERS_RATIO = PropertyConfiguration.getInteger("dse.tpc.timers_ratio", 5);
   private static final int NIO_IO_RATIO = PropertyConfiguration.getInteger("io.netty.ratioIO", 50);
   public static final boolean USE_EPOLL = PropertyConfiguration.getBoolean("cassandra.native.epoll.enabled", true) && Epoll.isAvailable();
   public static final boolean USE_AIO;
   public static final int READ_ASYNC_TIMEOUT_MILLIS;
   public static final AioCoordinator aioCoordinator;
   private static final AtomicLong schedulerRoundRobinIndex;
   private static final AtomicLong[] timerRoundRobinIndex;
   private static final TPCEventLoopGroup eventLoopGroup;
   private static final TPCScheduler[] perCoreSchedulers;
   private static final ArrayList<TPCTimer> timers;
   private static final IOScheduler ioScheduler;
   public static final TPCMetrics[] perCoreMetrics;

   public TPC() {
   }

   private static void register(TPCEventLoop loop) {
      int coreId = loop.thread().coreId();

      assert coreId >= 0 && coreId < TPCUtils.getNumCores();

      assert perCoreSchedulers[coreId] == null;

      TPCScheduler scheduler = new TPCScheduler(loop);
      perCoreSchedulers[coreId] = scheduler;
      if(timers.size() < getNumTimers()) {
         timers.add(new TPCHashedWheelTimer(scheduler));
      }

   }

   private static int getNumTimers() {
      return Math.max(1, TPCUtils.getNumCores() / TIMERS_RATIO);
   }

   private static void initRx() {
      RxJavaPlugins.setComputationSchedulerHandler((s) -> {
         return bestTPCScheduler();
      });
      RxJavaPlugins.setIoSchedulerHandler((s) -> {
         return ioScheduler();
      });
      RxJavaPlugins.setErrorHandler((e) -> {
         CassandraDaemon.defaultExceptionHandler.accept(Thread.currentThread(), e);
      });
      RxJavaPlugins.setScheduleHandler((runnable) -> {
         Runnable runnablex = TPCRunnable.wrap(runnable);
         Runnable runnablexx = LOG_CALLER_STACK_ON_EXCEPTION?new TPC.RunnableWithCallerThreadInfo(runnablex):runnablex;
         return (Runnable)runnablexx;
      });
      if(ENABLE_RX_SUBSCRIPTION_DEBUG) {
         RxSubscriptionDebugger.enable();
      }

   }

   public static void ensureInitialized(boolean initStageManager) {
      if(DatabaseDescriptor.setTPCInitialized()) {
         eventLoopGroup.eventLoops().forEach((e) -> {
            e.start();
         });
      }

      if(initStageManager) {
         StageManager.initDummy();
      }

   }

   public static TPCEventLoopGroup eventLoopGroup() {
      return eventLoopGroup;
   }

   public static TPCScheduler currentThreadTPCScheduler() {
      int coreId = TPCUtils.getCoreId();

      assert isValidCoreId(coreId) : "This method should not be called from a non-TPC thread.";

      return getForCore(coreId);
   }

   public static TPCScheduler bestTPCScheduler() {
      int coreId = TPCUtils.getCoreId();
      return isValidCoreId(coreId)?getForCore(coreId):getForCore(getNextCore());
   }

   public static TPCTimer bestTPCTimer() {
      return (TPCTimer)timers.get((int)(timerRoundRobinIndex[TPCUtils.getCoreId()].incrementAndGet() % (long)getNumTimers()));
   }

   public static TPCScheduler getNextTPCScheduler() {
      return getForCore(getNextCore());
   }

   public static int bestTPCCore() {
      int coreId = TPCUtils.getCoreId();
      return isValidCoreId(coreId)?coreId:getNextCore();
   }

   public static TPCEventLoop bestIOEventLoop() {
      return (TPCEventLoop)getForCore(aioCoordinator.getIOCore(bestTPCCore())).eventLoop;
   }

   public static IOScheduler ioScheduler() {
      return ioScheduler;
   }

   public static int getNextCore() {
      return (int)(schedulerRoundRobinIndex.getAndIncrement() % (long)TPCUtils.getNumCores());
   }

   public static TPCScheduler getForCore(int core) {
      return perCoreSchedulers[core];
   }

   public static boolean isValidCoreId(int coreId) {
      return coreId >= 0 && coreId < TPCUtils.getNumCores();
   }

   public static int getCoreForKey(Keyspace keyspace, DecoratedKey key) {
      return getCoreForKey(keyspace.getTPCBoundaries(), key);
   }

   public static int getCoreForKey(TPCBoundaries boundaries, DecoratedKey key) {
      if(boundaries == TPCBoundaries.NONE) {
         return 0;
      } else {
         Token keyToken = key.getToken();
         if(key.getPartitioner() != DatabaseDescriptor.getPartitioner()) {
            keyToken = DatabaseDescriptor.getPartitioner().getToken(key.getKey());
         }

         return boundaries.getCoreFor(keyToken);
      }
   }

   public static TPCScheduler getForKey(Keyspace keyspace, DecoratedKey key) {
      return getForCore(getCoreForKey(keyspace, key));
   }

   public static int getCoreForBound(Keyspace keyspace, PartitionPosition position) {
      return getCoreForBound(keyspace.getTPCBoundaries(), position);
   }

   public static int getCoreForBound(TPCBoundaries boundaries, PartitionPosition position) {
      return boundaries == TPCBoundaries.NONE?0:(position.getPartitioner() != DatabaseDescriptor.getPartitioner()?0:boundaries.getCoreFor(position.getToken()));
   }

   public static TPCScheduler getForBound(Keyspace keyspace, PartitionPosition position) {
      return getForCore(getCoreForBound(keyspace, position));
   }

   public static TPCMetrics metrics() {
      return perCoreMetrics[TPCUtils.getCoreId()];
   }

   public static TPCMetrics metrics(int forCore) {
      return perCoreMetrics[forCore];
   }

   public static void shutdown() {
      Iterator var0 = timers.iterator();

      while(var0.hasNext()) {
         TPCTimer timer = (TPCTimer)var0.next();
         timer.shutdown();
      }

      eventLoopGroup.shutdown();
   }

   public static List<Runnable> shutdownNow() {
      Iterator var0 = timers.iterator();

      while(var0.hasNext()) {
         TPCTimer timer = (TPCTimer)var0.next();
         timer.shutdown();
      }

      return eventLoopGroup.shutdownNow();
   }

   public static Executor getWrappedExecutor() {
      return (command) -> {
         bestTPCScheduler().getExecutor().execute(command);
      };
   }

   public static long nanoTimeSinceStartup() {
      return AbstractScheduledEventExecutor.nanoTime();
   }

   public static <T> CompletableFuture<T> withLock(ExecutableLock lock, Supplier<CompletableFuture<T>> action) {
      return lock.execute(action, bestTPCScheduler().getExecutor());
   }

   public static <T> T withLockBlocking(ExecutableLock lock, Callable<T> action) {
      try {
         return lock.executeBlocking(action);
      } catch (Exception var3) {
         throw Throwables.cleaned(var3);
      }
   }

   public static <T> CompletableFuture<T> withLocks(SortedMap<Long, ExecutableLock> locks, long startTimeMillis, long timeoutMillis, Supplier<CompletableFuture<T>> onLocksAcquired, java.util.function.Function<TimeoutException, RuntimeException> onTimeout) {
      CoordinatedAction<T> coordinator = new CoordinatedAction(onLocksAcquired, locks.size(), startTimeMillis, timeoutMillis, TimeUnit.MILLISECONDS);
      CompletableFuture<T> first = new CompletableFuture();
      CompletableFuture<T> prev = first;
      CompletableFuture<T> result = null;

      CompletableFuture current;
      for(Iterator var11 = locks.values().iterator(); var11.hasNext(); prev = current) {
         ExecutableLock lock = (ExecutableLock)var11.next();
         current = new CompletableFuture();
         result = prev.thenCompose((ignored) -> {
            return lock.execute(() -> {
               current.complete((Object)null);
               return coordinator.get();
            }, bestTPCScheduler().getExecutor());
         });
      }

      first.complete((Object)null);
      result = result.exceptionally((t) -> {
         if(t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
         }

         if(t instanceof TimeoutException) {
            throw (RuntimeException)onTimeout.apply((TimeoutException)t);
         } else {
            throw com.google.common.base.Throwables.propagate(t);
         }
      });
      return result;
   }

   static {
      USE_AIO = PropertyConfiguration.getBoolean("dse.io.aio.enabled", true) && Aio.isAvailable() && USE_EPOLL && (PropertyConfiguration.getBoolean("dse.io.aio.force", false) || DatabaseDescriptor.assumeDataDirectoriesOnSSD());
      READ_ASYNC_TIMEOUT_MILLIS = PropertyConfiguration.getInteger("dse.tpc.read_async_timeout_millis", 4000);
      aioCoordinator = new AioCoordinator(TPCUtils.getNumCores(), USE_AIO?DatabaseDescriptor.getTPCIOCores():0, DatabaseDescriptor.getIOGlobalQueueDepth());
      schedulerRoundRobinIndex = new AtomicLong(0L);
      timerRoundRobinIndex = new AtomicLong[TPCUtils.getNumCores() + 1];
      perCoreSchedulers = new TPCScheduler[TPCUtils.getNumCores()];
      timers = new ArrayList(getNumTimers());
      ioScheduler = new IOScheduler();
      perCoreMetrics = new TPCMetrics[TPCUtils.getNumCores() + 1];

      int i;
      for(i = 0; i <= TPCUtils.getNumCores(); ++i) {
         perCoreMetrics[i] = new TPCMetricsAndLimits();
         timerRoundRobinIndex[i] = new AtomicLong();
      }

      if(USE_EPOLL) {
         eventLoopGroup = new EpollTPCEventLoopGroup(TPCUtils.getNumCores());
         logger.info("Created {} epoll event loops.", Integer.valueOf(TPCUtils.getNumCores()));
      } else {
         NioTPCEventLoopGroup group = new NioTPCEventLoopGroup(TPCUtils.getNumCores());
         group.setIoRatio(NIO_IO_RATIO);
         eventLoopGroup = group;
         logger.info("Created {} NIO event loops (with I/O ratio set to {}).", Integer.valueOf(TPCUtils.getNumCores()), Integer.valueOf(NIO_IO_RATIO));
      }

      eventLoopGroup.eventLoops().forEach(TPC::register);
      logger.info("Created {} TPC timers due to configured ratio of {}.", Integer.valueOf(timers.size()), Integer.valueOf(TIMERS_RATIO));
      initRx();

      for(i = 0; i < TPCUtils.getNumCores(); ++i) {
         new TPCTotalMetrics(perCoreMetrics[i], "internal", "TPC/" + i);
      }

      new TPCTotalMetrics(perCoreMetrics[TPCUtils.getNumCores()], "internal", "TPC/other");
      TPCTaskType[] var5 = TPCTaskType.values();
      int var1 = var5.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         TPCTaskType type = var5[var2];
         new TPCAggregatedStageMetrics(perCoreMetrics, type, "internal", "TPC/all");
      }

   }

   private abstract static class NettyTime extends AbstractScheduledEventExecutor {
      private NettyTime() {
      }

      public static long nanoSinceStartup() {
         return AbstractScheduledEventExecutor.nanoTime();
      }
   }

   private static final class RunnableWithCallerThreadInfo implements Runnable {
      private final Runnable runnable;
      private final FBUtilities.Debug.ThreadInfo threadInfo;

      RunnableWithCallerThreadInfo(Runnable runnable) {
         this.runnable = runnable;
         this.threadInfo = new FBUtilities.Debug.ThreadInfo();
      }

      public void run() {
         try {
            this.runnable.run();
         } catch (Throwable var2) {
            TPC.logger.error("Got exception {} with message <{}> when running Rx task. Caller's thread stack:\n{}", new Object[]{var2.getClass().getName(), var2.getMessage(), FBUtilities.Debug.getStackTrace(this.threadInfo)});
            throw var2;
         }
      }
   }
}
