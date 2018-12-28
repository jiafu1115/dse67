package org.apache.cassandra.db.compaction;

import java.util.Map;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public interface CompactionEventListener {
   void handleCompactionEvent(CompactionEvent var1, CompactionIterator var2, Map<SSTableReader, AbstractCompactionStrategy> var3, long var4);
}
