package org.apache.cassandra.service;

import com.sun.management.GarbageCollectionNotificationInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Arrays;

public final class GCState {
   public final GarbageCollectorMXBean gcBean;
   public final boolean assumeGCIsPartiallyConcurrent;
   public final boolean assumeGCIsOldGen;
   private String[] keys;
   public long lastGcTotalDuration = 0L;

   public GCState(GarbageCollectorMXBean gcBean, boolean assumeGCIsPartiallyConcurrent, boolean assumeGCIsOldGen) {
      this.gcBean = gcBean;
      this.assumeGCIsPartiallyConcurrent = assumeGCIsPartiallyConcurrent;
      this.assumeGCIsOldGen = assumeGCIsOldGen;
   }

   String[] keys(GarbageCollectionNotificationInfo info) {
      if(this.keys != null) {
         return this.keys;
      } else {
         this.keys = (String[])info.getGcInfo().getMemoryUsageBeforeGc().keySet().toArray(new String[0]);
         Arrays.sort(this.keys);
         return this.keys;
      }
   }
}
