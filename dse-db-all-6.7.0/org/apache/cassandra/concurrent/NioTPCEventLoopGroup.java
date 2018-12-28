package org.apache.cassandra.concurrent;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import io.netty.channel.EventLoop;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.Executor;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public class NioTPCEventLoopGroup extends NioEventLoopGroup implements TPCEventLoopGroup {
   private final UnmodifiableArrayList<NioTPCEventLoopGroup.SingleCoreEventLoop> eventLoops = UnmodifiableArrayList.copyOf(Iterables.transform(this, (e) -> {
      return (NioTPCEventLoopGroup.SingleCoreEventLoop)e;
   }));

   NioTPCEventLoopGroup(int nThreads) {
      super(nThreads, TPCThread.newTPCThreadFactory());
   }

   public UnmodifiableArrayList<? extends TPCEventLoop> eventLoops() {
      return this.eventLoops;
   }

   protected EventLoop newChild(Executor executor, Object... args) {
      assert executor instanceof TPCThread.TPCThreadsCreator;

      SelectorProvider selectorProvider = (SelectorProvider)args[0];
      SelectStrategyFactory selectStrategyFactory = (SelectStrategyFactory)args[1];
      RejectedExecutionHandler rejectedExecutionHandler = (RejectedExecutionHandler)args[2];
      return new NioTPCEventLoopGroup.SingleCoreEventLoop(this, (TPCThread.TPCThreadsCreator)executor, selectorProvider, selectStrategyFactory.newSelectStrategy(), rejectedExecutionHandler);
   }

   private static class SingleCoreEventLoop extends NioEventLoop implements TPCEventLoop {
      private final NioTPCEventLoopGroup parent;
      private final TPCThread thread;

      private SingleCoreEventLoop(NioTPCEventLoopGroup parent, TPCThread.TPCThreadsCreator executor, SelectorProvider selectorProvider, SelectStrategy selectStrategy, RejectedExecutionHandler rejectedExecutionHandler) {
         super(parent, executor, selectorProvider, selectStrategy, rejectedExecutionHandler);
         this.parent = parent;
         Futures.getUnchecked(this.submit(() -> {
         }));
         this.thread = executor.lastCreatedThread();

         assert this.thread != null;

      }

      public TPCThread thread() {
         return this.thread;
      }

      public TPCEventLoopGroup parent() {
         return this.parent;
      }

      public boolean canExecuteImmediately(TPCTaskType taskType) {
         return taskType.pendable()?false:this.coreId() == TPCUtils.getCoreId();
      }

      public boolean shouldBackpressure(boolean remote) {
         return false;
      }
   }
}
