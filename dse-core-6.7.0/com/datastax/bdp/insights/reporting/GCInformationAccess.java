package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.events.GCInformation;
import com.datastax.insights.client.InsightsClient;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.openmbean.CompositeData;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.GCState;
import org.gridkit.jvmtool.GcCpuUsageMonitor;
import org.gridkit.jvmtool.PerfCounterGcCpuUsageMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCInformationAccess implements NotificationListener {
   private static final Logger logger = LoggerFactory.getLogger(GCInformationAccess.class);
   private final InsightsClient insightsClient;
   private String oldGenPoolName;
   private GcCpuUsageMonitor gcCpuUsageMonitor;
   private final Map<String, GCState> gcStates = new HashMap();

   public GCInformationAccess(InsightsClient insightsClient) {
      this.insightsClient = insightsClient;
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      Iterator var4;
      try {
         ObjectName gcName = new ObjectName("java.lang:type=GarbageCollector,*");
         var4 = mbs.queryNames(gcName, (QueryExp)null).iterator();

         while(var4.hasNext()) {
            ObjectName name = (ObjectName)var4.next();
            GarbageCollectorMXBean gc = (GarbageCollectorMXBean)ManagementFactory.newPlatformMXBeanProxy(mbs, name.getCanonicalName(), GarbageCollectorMXBean.class);
            this.gcStates.put(gc.getName(), new GCState(gc, GCInspector.assumeGCIsPartiallyConcurrent(gc), GCInspector.assumeGCIsOldGen(gc)));
         }
      } catch (IOException | MalformedObjectNameException var8) {
         logger.warn("Error getting quering MXBeans", var8);
      }

      boolean foundOldGen = false;
      var4 = ManagementFactory.getMemoryPoolMXBeans().iterator();

      while(var4.hasNext()) {
         MemoryPoolMXBean mbean = (MemoryPoolMXBean)var4.next();
         if(this.isOldGenPool(mbean.getName())) {
            this.oldGenPoolName = mbean.getName();
            foundOldGen = true;
            break;
         }
      }

      if(!foundOldGen) {
         logger.warn("Did not find GC old generation pool. Promoted bytes won't be available");
      }

      try {
         this.gcCpuUsageMonitor = new PerfCounterGcCpuUsageMonitor((long)this.getPid());
      } catch (RuntimeException | Error var7) {
         logger.warn("Could not get GC CPU utilization data: {}", var7.getMessage());
      }

   }

   GCInformationAccess(InsightsClient insightsClient, Map<String, GCState> gcStates, String oldGenPoolName, GcCpuUsageMonitor gcCpuUsageMonitor) {
      this.insightsClient = insightsClient;
      this.oldGenPoolName = oldGenPoolName;
      this.gcCpuUsageMonitor = gcCpuUsageMonitor;
      this.gcStates.clear();
      Iterator var5 = gcStates.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<String, GCState> gcState = (Entry)var5.next();
         this.gcStates.put(gcState.getKey(), gcState.getValue());
      }

   }

   private boolean isOldGenPool(String name) {
      return name.contains("Old Gen") || name.contains("Tenured Gen");
   }

   private int getPid() {
      String name = ManagementFactory.getRuntimeMXBean().getName();
      name = name.substring(0, name.indexOf("@"));
      return Integer.parseInt(name);
   }

   private long getDuration(String gcName, GcInfo gcInfo) {
      GCState gcState = (GCState)this.gcStates.get(gcName);
      return gcState != null?GCInspector.getDuration(gcInfo, gcState):gcInfo.getDuration();
   }

   private Map<String, Long> getMemoryUsed(Map<String, MemoryUsage> memoryUsage) {
      return (Map)memoryUsage.entrySet().stream().collect(Collectors.toMap(Entry::getKey, (e) -> {
         return Long.valueOf(((MemoryUsage)e.getValue()).getUsed());
      }));
   }

   public void handleNotification(Notification notification, Object handback) {
      String type = notification.getType();
      if(type.equals("com.sun.management.gc.notification")) {
         CompositeData cd = (CompositeData)notification.getUserData();
         GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
         String gcName = info.getGcName();
         GcInfo gcInfo = info.getGcInfo();
         long duration = this.getDuration(gcName, gcInfo);
         Map<String, Long> beforeMemoryUsage = this.getMemoryUsed(gcInfo.getMemoryUsageBeforeGc());
         Map<String, Long> afterMemoryUsage = this.getMemoryUsed(gcInfo.getMemoryUsageAfterGc());
         long promotedBytes = 0L;
         if(this.oldGenPoolName != null && afterMemoryUsage.containsKey(this.oldGenPoolName)) {
            promotedBytes = Math.max(0L, ((Long)afterMemoryUsage.get(this.oldGenPoolName)).longValue() - ((Long)beforeMemoryUsage.get(this.oldGenPoolName)).longValue());
         }

         long youngGcCpu = 0L;
         long oldGcCpu = 0L;
         if(this.gcCpuUsageMonitor != null) {
            youngGcCpu = this.gcCpuUsageMonitor.getYoungGcCpu();
            oldGcCpu = this.gcCpuUsageMonitor.getOldGcCpu();
         }

         GCInformation gcInformation = new GCInformation(gcName, duration, beforeMemoryUsage, afterMemoryUsage, promotedBytes, youngGcCpu, oldGcCpu);

         try {
            this.insightsClient.report(gcInformation);
         } catch (Exception var20) {
            logger.warn("Error reporting GC information", var20);
         }
      }

   }
}
