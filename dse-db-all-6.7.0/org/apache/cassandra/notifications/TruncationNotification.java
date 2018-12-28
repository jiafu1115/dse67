package org.apache.cassandra.notifications;

public class TruncationNotification implements INotification {
   public final long truncatedAt;

   public TruncationNotification(long truncatedAt) {
      this.truncatedAt = truncatedAt;
   }
}
