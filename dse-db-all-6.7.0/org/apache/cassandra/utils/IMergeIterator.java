package org.apache.cassandra.utils;

import java.util.Iterator;

public interface IMergeIterator<In, Out> extends CloseableIterator<Out> {
   Iterable<? extends Iterator<In>> iterators();
}
