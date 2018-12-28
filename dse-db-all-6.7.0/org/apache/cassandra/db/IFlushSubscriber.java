package org.apache.cassandra.db;

import java.util.List;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public interface IFlushSubscriber {
   void onFlush(TableMetadata var1, boolean var2, ColumnFamilyStore.FlushReason var3, Memtable var4, List<SSTableReader> var5, long var6);
}
