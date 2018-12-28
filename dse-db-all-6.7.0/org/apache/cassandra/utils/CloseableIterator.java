package org.apache.cassandra.utils;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
   void close();
}
