package com.datastax.bdp.insights.reporting;

import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationFilter;
import javax.management.ObjectName;
import javax.management.QueryExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class GCInformationReporter {
   private static final Logger logger = LoggerFactory.getLogger(GCInformationReporter.class);
   private final GCInformationAccess gcInformationAccess;

   @Inject
   public GCInformationReporter(InsightsClient insightsClient) {
      this.gcInformationAccess = new GCInformationAccess(insightsClient);
   }

   public void startReportingGCInformation() {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();

      try {
         ObjectName gcName = new ObjectName("java.lang:type=GarbageCollector,*");
         Iterator var3 = server.queryNames(gcName, (QueryExp)null).iterator();

         while(var3.hasNext()) {
            ObjectName name = (ObjectName)var3.next();
            server.addNotificationListener(name, this.gcInformationAccess, (NotificationFilter)null, (Object)null);
         }
      } catch (InstanceNotFoundException | MalformedObjectNameException var5) {
         logger.warn("Error starting GC information reporting", var5);
      }

   }

   public void stopReportingGCInformation() {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();

      try {
         ObjectName gcName = new ObjectName("java.lang:type=GarbageCollector,*");
         Iterator var3 = server.queryNames(gcName, (QueryExp)null).iterator();

         while(var3.hasNext()) {
            ObjectName name = (ObjectName)var3.next();
            server.removeNotificationListener(name, this.gcInformationAccess, (NotificationFilter)null, (Object)null);
         }
      } catch (InstanceNotFoundException | ListenerNotFoundException | MalformedObjectNameException var5) {
         logger.warn("Error canceling GC information reporting:", var5);
      }

   }
}
