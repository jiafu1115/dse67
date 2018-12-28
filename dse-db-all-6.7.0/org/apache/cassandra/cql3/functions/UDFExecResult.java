package org.apache.cassandra.cql3.functions;

final class UDFExecResult<T> {
   T result;
   long cpuTime;
   long allocatedBytes;
   boolean warnCpuTimeExceeded;
   boolean failCpuTimeExceeded;
   boolean warnAllocatedBytesExceeded;
   boolean failAllocatedBytesExceeded;

   UDFExecResult() {
   }
}
