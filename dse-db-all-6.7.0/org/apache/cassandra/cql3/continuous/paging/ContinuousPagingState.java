package org.apache.cassandra.cql3.continuous.paging;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import java.util.function.Supplier;
import org.apache.cassandra.config.ContinuousPagingConfig;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

public class ContinuousPagingState {
   public final TimeSource timeSource;
   public final ContinuousPagingConfig config;
   public final ContinuousPagingExecutor executor;
   public final Supplier<Channel> channel;
   public final int averageRowSize;
   public final ContinuousPagingEventHandler handler;

   public ContinuousPagingState(ContinuousPagingConfig config, ContinuousPagingExecutor executor, Supplier<Channel> channel, int averageRowSize, ContinuousPagingEventHandler handler) {
      this(new SystemTimeSource(), config, executor, channel, averageRowSize, handler);
   }

   @VisibleForTesting
   ContinuousPagingState(TimeSource timeSource, ContinuousPagingConfig config, ContinuousPagingExecutor executor, Supplier<Channel> channel, int averageRowSize, ContinuousPagingEventHandler handler) {
      this.timeSource = timeSource;
      this.config = config;
      this.executor = executor;
      this.channel = channel;
      this.averageRowSize = averageRowSize;
      this.handler = handler;
   }
}
