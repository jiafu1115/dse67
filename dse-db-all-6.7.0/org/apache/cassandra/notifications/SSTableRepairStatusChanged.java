package org.apache.cassandra.notifications;

import java.util.Collection;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SSTableRepairStatusChanged implements INotification {
   public final Collection<SSTableReader> sstables;

   public SSTableRepairStatusChanged(Collection<SSTableReader> repairStatusChanged) {
      this.sstables = repairStatusChanged;
   }
}
