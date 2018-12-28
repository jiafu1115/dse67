package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public final class NoopCompactionStrategy extends AbstractCompactionStrategy {
   public NoopCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      super(cfs, options);
   }

   public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      return null;
   }

   public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
      return Collections.emptyList();
   }

   public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore) {
      return null;
   }

   public int getEstimatedRemainingTasks() {
      return 0;
   }

   public long getMaxSSTableBytes() {
      return 0L;
   }

   public void addSSTable(SSTableReader added) {
   }

   public void removeSSTable(SSTableReader sstable) {
   }

   protected Set<SSTableReader> getSSTables() {
      return Collections.emptySet();
   }
}
