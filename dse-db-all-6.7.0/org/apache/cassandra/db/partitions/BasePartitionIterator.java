package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.utils.CloseableIterator;

public interface BasePartitionIterator<I extends BaseRowIterator<?>> extends CloseableIterator<I> {
   void close();
}
