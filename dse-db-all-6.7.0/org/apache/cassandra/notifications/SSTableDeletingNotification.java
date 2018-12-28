package org.apache.cassandra.notifications;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SSTableDeletingNotification implements INotification {
   public final SSTableReader deleting;

   public SSTableDeletingNotification(SSTableReader deleting) {
      this.deleting = deleting;
   }
}
