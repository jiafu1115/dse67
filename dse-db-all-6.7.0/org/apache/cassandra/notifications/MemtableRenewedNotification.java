package org.apache.cassandra.notifications;

import org.apache.cassandra.db.Memtable;

public class MemtableRenewedNotification implements INotification {
   public final Memtable renewed;

   public MemtableRenewedNotification(Memtable renewed) {
      this.renewed = renewed;
   }
}
