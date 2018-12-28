package com.datastax.bdp.server;

import com.datastax.bdp.snitch.Workload;
import com.google.common.annotations.VisibleForTesting;
import java.util.EnumSet;
import java.util.Set;
import org.apache.cassandra.config.PropertyConfiguration;

public class CoreSystemInfo {
   private static final boolean isSearchNode = PropertyConfiguration.getBoolean("search-service");
   private static final boolean isSparkNode = PropertyConfiguration.getBoolean("spark-trackers");
   private static final boolean isGraphNode = PropertyConfiguration.getBoolean("graph-enabled");
   private static final boolean isAdvRepEnabled = PropertyConfiguration.getBoolean("advrep");
   private static final Set<Workload> workloads = unmemoizedWorkloads();

   public CoreSystemInfo() {
   }

   @VisibleForTesting
   static Set<Workload> unmemoizedWorkloads() {
      Set<Workload> w = EnumSet.of(Workload.Cassandra);
      if(isSparkNode()) {
         w.add(Workload.Analytics);
      }

      if(isSearchNode()) {
         w.add(Workload.Search);
      }

      if(isGraphNode()) {
         w.add(Workload.Graph);
      }

      return w;
   }

   public static boolean isAdvRepEnabled() {
      return isAdvRepEnabled;
   }

   public static boolean isSearchNode() {
      return isSearchNode;
   }

   public static boolean isSparkNode() {
      return isSparkNode;
   }

   public static boolean isGraphNode() {
      return isGraphNode;
   }

   public static Set<Workload> getWorkloads() {
      return workloads;
   }
}
