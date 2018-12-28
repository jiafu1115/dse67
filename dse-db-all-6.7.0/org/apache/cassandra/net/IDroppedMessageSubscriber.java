package org.apache.cassandra.net;

import java.util.List;

public interface IDroppedMessageSubscriber {
   void onMessageDropped(List<DroppedMessages.DroppedMessageGroupStats> var1);
}
