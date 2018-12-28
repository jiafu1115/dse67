package org.apache.cassandra.db.compaction;

import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class LeveledCompactionTask extends CompactionTask {
   private final int level;
   private final long maxSSTableBytes;
   private final boolean majorCompaction;

   public LeveledCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int level, int gcBefore, long maxSSTableBytes, boolean majorCompaction) {
      super(cfs, txn, gcBefore);
      this.level = level;
      this.maxSSTableBytes = maxSSTableBytes;
      this.majorCompaction = majorCompaction;
   }

   public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables) {
      return (CompactionAwareWriter)(this.majorCompaction?new MajorLeveledCompactionWriter(cfs, directories, txn, nonExpiredSSTables, this.maxSSTableBytes, false):new MaxSSTableSizeWriter(cfs, directories, txn, nonExpiredSSTables, this.maxSSTableBytes, this.getLevel(), false));
   }

   protected boolean partialCompactionsAcceptable() {
      return this.level == 0;
   }

   protected int getLevel() {
      return this.level;
   }
}
