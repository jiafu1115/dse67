package org.apache.cassandra.notifications;

import java.util.Collection;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SSTableListChangedNotification implements INotification {
   public final Collection<SSTableReader> removed;
   public final Collection<SSTableReader> added;
   public final OperationType compactionType;

   public SSTableListChangedNotification(Collection<SSTableReader> added, Collection<SSTableReader> removed, OperationType compactionType) {
      this.removed = removed;
      this.added = added;
      this.compactionType = compactionType;
   }
}
