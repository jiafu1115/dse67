package org.apache.cassandra.dht.tokenallocator;

interface ReplicationStrategy<Unit> {
   int replicas();

   Object getGroup(Unit var1);
}
