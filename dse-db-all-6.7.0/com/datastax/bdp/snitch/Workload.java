package com.datastax.bdp.snitch;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum Workload {
   Unknown,
   Analytics,
   Cassandra,
   Search,
   Graph;

   private static Logger logger = LoggerFactory.getLogger(Workload.class);
   private static final Comparator<String> COMP = String::compareTo;

   private Workload() {
   }

   public boolean isCompatibleWith(Set<Workload> workloads) {
      return null != workloads && workloads.contains(this);
   }

   public static Set<String> toStringSet(Set<Workload> workloads) {
      Set<String> workloadNames = new HashSet();
      if(!isDefined(workloads)) {
         workloadNames.add(Unknown.name());
         return workloadNames;
      } else {
         workloadNames.addAll((Collection)workloads.stream().map(Enum::name).collect(Collectors.toList()));
         return Collections.unmodifiableSet(workloadNames);
      }
   }

   public static Set<Workload> fromStringSet(Set<String> workloadNames, InetAddress endpoint) {
      Set<Workload> workloads = fromStringSet(workloadNames);
      if(workloads.contains(Unknown)) {
         logger.warn("Couldn't determine workloads for {} from value {}", endpoint, workloadNames == null?"NULL":workloads);
      }

      return Collections.unmodifiableSet(workloads);
   }

   public static Set<Workload> fromStringSet(Set<String> workloadNames) {
      Set<Workload> workloads = EnumSet.noneOf(Workload.class);
      if(null != workloadNames && !workloadNames.isEmpty()) {
         try {
            Iterator var2 = workloadNames.iterator();

            while(var2.hasNext()) {
               String workloadName = (String)var2.next();
               workloads.add(valueOf(workloadName));
            }
         } catch (Exception var4) {
            workloads.add(Unknown);
         }

         return Collections.unmodifiableSet(workloads);
      } else {
         workloads.add(Unknown);
         return workloads;
      }
   }

   public static Set<Workload> fromString(String workloadNames, InetAddress endpoint) {
      Set<Workload> workloads = fromString(workloadNames);
      if(workloads.contains(Unknown)) {
         logger.warn("Couldn't determine workloads for {} from value {}", endpoint, workloadNames == null?"NULL":workloads);
      }

      return Collections.unmodifiableSet(workloads);
   }

   public static Set<Workload> fromString(String workloadNames) {
      EnumSet workloads = EnumSet.noneOf(Workload.class);

      try {
         Workload[] var2 = values();
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Workload workload = var2[var4];
            if(workloadNames.contains(workload.name())) {
               workloads.add(workload);
            }
         }
      } catch (Exception var6) {
         ;
      }

      if(workloads.isEmpty()) {
         workloads.add(Unknown);
      }

      return Collections.unmodifiableSet(workloads);
   }

   public static String workloadNames(Set<Workload> workloads) {
      return !isDefined(workloads)?Unknown.name():(String)workloads.stream().map(Enum::name).sorted(COMP.reversed()).collect(Collectors.joining());
   }

   public static String legacyWorkloadName(Set<Workload> workloads) {
      if(!isDefined(workloads)) {
         return Unknown.name();
      } else {
         String legacyWorkload = workloadNames(workloads).replace(Graph.name(), "");
         legacyWorkload = !legacyWorkload.equals(Cassandra.name())?legacyWorkload.replace(Cassandra.name(), ""):legacyWorkload;
         return "".equals(legacyWorkload)?Unknown.name():legacyWorkload;
      }
   }

   public static boolean isDefined(Set<Workload> workloads) {
      return null != workloads && !workloads.isEmpty();
   }

   public static Set<Workload> fromLegacyWorkloadName(String legacyWorkload, boolean graphEnabled) {
      if(null != legacyWorkload && !legacyWorkload.isEmpty() && !Unknown.name().equals(legacyWorkload)) {
         Set<Workload> workloads = EnumSet.of(Cassandra);
         byte var4 = -1;
         switch(legacyWorkload.hashCode()) {
         case -1822469688:
            if(legacyWorkload.equals("Search")) {
               var4 = 1;
            }
            break;
         case -1217435224:
            if(legacyWorkload.equals("Cassandra")) {
               var4 = 0;
            }
            break;
         case -264967522:
            if(legacyWorkload.equals("SearchAnalytics")) {
               var4 = 3;
            }
            break;
         case 310950758:
            if(legacyWorkload.equals("Analytics")) {
               var4 = 2;
            }
         }

         switch(var4) {
         case 0:
            break;
         case 1:
            workloads.add(Search);
            break;
         case 2:
            workloads.add(Analytics);
            break;
         case 3:
            workloads.add(Search);
            workloads.add(Analytics);
            break;
         default:
            return Collections.unmodifiableSet(EnumSet.of(Unknown));
         }

         if(graphEnabled) {
            workloads.add(Graph);
         }

         return Collections.unmodifiableSet(workloads);
      } else {
         return Collections.unmodifiableSet(EnumSet.of(Unknown));
      }
   }
}
