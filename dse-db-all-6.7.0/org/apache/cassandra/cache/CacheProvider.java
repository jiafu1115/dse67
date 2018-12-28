package org.apache.cassandra.cache;

public interface CacheProvider<K, V> {
   ICache<K, V> create();
}
