package org.apache.cassandra.concurrent;

import io.netty.channel.EventLoopGroup;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public interface TPCEventLoopGroup extends EventLoopGroup {
   UnmodifiableArrayList<? extends TPCEventLoop> eventLoops();
}
