package org.apache.cassandra.streaming;

import com.google.common.util.concurrent.FutureCallback;

public interface StreamEventHandler extends FutureCallback<StreamState> {
   void handleStreamEvent(StreamEvent var1);
}
