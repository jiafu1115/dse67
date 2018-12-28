package org.apache.cassandra.gms;

public enum ApplicationState {
   STATUS,
   LOAD,
   SCHEMA,
   DC,
   RACK,
   RELEASE_VERSION,
   REMOVAL_COORDINATOR,
   INTERNAL_IP,
   NATIVE_TRANSPORT_ADDRESS,
   DSE_GOSSIP_STATE,
   SEVERITY,
   NET_VERSION,
   HOST_ID,
   TOKENS,
   NATIVE_TRANSPORT_READY,
   NATIVE_TRANSPORT_PORT,
   NATIVE_TRANSPORT_PORT_SSL,
   STORAGE_PORT,
   STORAGE_PORT_SSL,
   JMX_PORT,
   SCHEMA_COMPATIBILITY_VERSION,
   X1,
   X2,
   X3,
   X4,
   X5,
   X6,
   X7,
   X8,
   X9,
   X10;

   private ApplicationState() {
   }
}
