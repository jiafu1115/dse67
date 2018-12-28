package com.datastax.bdp.db.upgrade;

import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterVersionBarrier {
   private static final Logger logger = LoggerFactory.getLogger(ClusterVersionBarrier.class);
   private volatile ClusterVersionBarrier.ClusterVersionInfo current;
   private final CopyOnWriteArrayList<ClusterVersionBarrier.ClusterVersionListener> listeners;
   private volatile int scheduled;
   private static final AtomicIntegerFieldUpdater<ClusterVersionBarrier> scheduledUpdater = AtomicIntegerFieldUpdater.newUpdater(ClusterVersionBarrier.class, "scheduled");
   private boolean ready;
   private final Supplier<Iterable<InetAddress>> endpointsSupplier;
   private final Function<InetAddress, ClusterVersionBarrier.EndpointInfo> endpointInfoFunction;

   public ClusterVersionBarrier(Supplier<Iterable<InetAddress>> endpointsSupplier, Function<InetAddress, ClusterVersionBarrier.EndpointInfo> endpointInfoFunction) {
      this.current = new ClusterVersionBarrier.ClusterVersionInfo(ProductVersion.Version.nullVersion, ProductVersion.Version.nullVersion, ProductVersion.Version.nullVersion, ProductVersion.Version.nullVersion, false, UUID.randomUUID(), true);
      this.listeners = new CopyOnWriteArrayList();
      this.endpointsSupplier = endpointsSupplier;
      this.endpointInfoFunction = endpointInfoFunction;
   }

   public void scheduleUpdateVersions() {
      if(scheduledUpdater.compareAndSet(this, 0, 1)) {
         ScheduledExecutors.nonPeriodicTasks.execute(() -> {
            scheduledUpdater.set(this, 0);
            this.updateVersionsBlocking();
         });
      }

   }

   public ClusterVersionBarrier.ClusterVersionInfo currentClusterVersionInfo() {
      return this.current;
   }

   @VisibleForTesting
   boolean hasScheduledUpdate() {
      return scheduledUpdater.get(this) != 0;
   }

   public synchronized void updateVersionsBlocking() {
      ClusterVersionBarrier.ClusterVersionInfo versions = this.computeClusterVersionInfo();
      if(!this.current.equals(versions) && this.ready) {
         this.current = versions;
         logger.trace("updateVersionsBlocking - calling listeners");
         Iterator var2 = this.listeners.iterator();

         while(var2.hasNext()) {
            ClusterVersionBarrier.ClusterVersionListener listener = (ClusterVersionBarrier.ClusterVersionListener)var2.next();
            this.callListener(listener, versions);
         }
      }

   }

   private void callListener(ClusterVersionBarrier.ClusterVersionListener listener, ClusterVersionBarrier.ClusterVersionInfo versions) {
      try {
         listener.clusterVersionUpdated(versions);
      } catch (Exception var4) {
         logger.error("Unexpected error in cluster-version listener {} for version {}", new Object[]{listener.getClass().getName(), versions, var4});
         JVMStabilityInspector.inspectThrowable(var4);
      }

   }

   private ClusterVersionBarrier.ClusterVersionInfo computeClusterVersionInfo() {
      logger.trace("computeClusterVersionInfo - start computing");
      ProductVersion.Version minDse = null;
      ProductVersion.Version maxDse = null;
      ProductVersion.Version minOss = null;
      ProductVersion.Version maxOss = null;
      boolean allDSE = true;
      UUID schema = null;
      boolean schemaAgreement = true;
      Iterator var8 = ((Iterable)this.endpointsSupplier.get()).iterator();

      while(true) {
         ClusterVersionBarrier.EndpointInfo epInfo;
         do {
            if(!var8.hasNext()) {
               if(minOss == null) {
                  minOss = ProductVersion.getReleaseVersion();
               }

               if(maxOss == null) {
                  maxOss = ProductVersion.getReleaseVersion();
               }

               if(minDse == null) {
                  minDse = ProductVersion.getDSEVersion();
               }

               if(maxDse == null) {
                  maxDse = ProductVersion.getDSEVersion();
               }

               ClusterVersionBarrier.ClusterVersionInfo clusterVersionInfo = new ClusterVersionBarrier.ClusterVersionInfo(minDse, maxDse, minOss, maxOss, allDSE, schemaAgreement?schema:null, schemaAgreement);
               logger.trace("computeClusterVersionInfo - result={}", clusterVersionInfo);
               return clusterVersionInfo;
            }

            InetAddress ep = (InetAddress)var8.next();
            epInfo = (ClusterVersionBarrier.EndpointInfo)this.endpointInfoFunction.apply(ep);
            logger.trace("computeClusterVersionInfo - endpoint {} : {}", ep, epInfo);
         } while(epInfo == null);

         ProductVersion.Version epVer = epInfo.cassandraVersion;
         if(epVer != null) {
            if(minOss == null || epVer.compareTo(minOss) < 0) {
               minOss = epVer;
            }

            if(maxOss == null || epVer.compareTo(maxOss) > 0) {
               maxOss = epVer;
            }
         }

         epVer = epInfo.dseVersion;
         if(epVer == null) {
            allDSE = false;
         } else {
            if(minDse == null || epVer.compareTo(minDse) < 0) {
               minDse = epVer;
            }

            if(maxDse == null || epVer.compareTo(maxDse) > 0) {
               maxDse = epVer;
            }
         }

         UUID schemaVer = epInfo.schemaVersion;
         if(schemaVer != null) {
            if(schema == null) {
               schema = schemaVer;
            } else if(!schema.equals(schemaVer)) {
               schemaAgreement = false;
            }
         } else {
            schemaAgreement = false;
         }
      }
   }

   public synchronized void onLocalNodeReady() {
      if(!this.ready) {
         this.ready = true;
         this.updateVersionsBlocking();
      }

   }

   public synchronized void register(ClusterVersionBarrier.ClusterVersionListener listener) {
      if(this.ready) {
         this.callListener(listener, this.current);
      }

      this.listeners.add(listener);
   }

   public synchronized void unregister(ClusterVersionBarrier.ClusterVersionListener listener) {
      this.listeners.remove(listener);
   }

   @VisibleForTesting
   public synchronized void removeAllListeners() {
      this.listeners.clear();
   }

   public boolean runAtDseVersion(final ProductVersion.Version minDseVersion, final Consumer<ClusterVersionBarrier.ClusterVersionInfo> runnable) {
      ClusterVersionBarrier.ClusterVersionInfo curr = this.current;
      if(this.ready && minDseVersion.compareTo(curr.minDse) <= 0) {
         logger.debug("runAtDseVersion({}, {}) fires immediately, as condition is met with current versions = {}", new Object[]{minDseVersion, runnable.getClass().getName(), curr});
         runnable.accept(curr);
         return true;
      } else {
         logger.debug("runAtDseVersion({}, {}) delayed until condition is met - current versions = {}", new Object[]{minDseVersion, runnable.getClass().getName(), curr});
         this.register(new ClusterVersionBarrier.ClusterVersionListener() {
            public void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo versionInfo) {
               if(versionInfo.allDSE && minDseVersion.compareTo(versionInfo.minDse) <= 0) {
                  ClusterVersionBarrier.logger.debug("delayed runAtDseVersion({}, {}) fires, since condition is met with current versions = {}", new Object[]{minDseVersion, runnable.getClass().getName(), versionInfo});

                  try {
                     runnable.accept(versionInfo);
                  } catch (Exception var3) {
                     ClusterVersionBarrier.logger.error("Unexpected error in run-at-version callback {} for version {}", new Object[]{runnable.getClass().getName(), versionInfo, var3});
                     JVMStabilityInspector.inspectThrowable(var3);
                  }

                  ClusterVersionBarrier.this.unregister(this);
               } else {
                  ClusterVersionBarrier.logger.debug("delayed runAtDseVersion({}, {}), since condition is still not met with current versions = {}", new Object[]{minDseVersion, runnable.getClass().getName(), versionInfo});
               }

            }
         });
         return false;
      }
   }

   public void runAtDseVersion(ProductVersion.Version minDseVersion, String standByMessage, String readyMessage, Runnable runnable) {
      if(!this.runAtDseVersion(minDseVersion, (v) -> {
         logger.info("Cluster is now at DSE version {} (required {}). {}", new Object[]{v.minDse, minDseVersion, readyMessage});
         runnable.run();
      })) {
         logger.info("Cluster hasn't been completely upgraded to DSE version {} or higher yet. {}", minDseVersion, standByMessage);
      }

   }

   public interface ClusterVersionListener {
      void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo var1);
   }

   public static final class ClusterVersionInfo {
      public final ProductVersion.Version minDse;
      public final ProductVersion.Version maxDse;
      public final ProductVersion.Version minOss;
      public final ProductVersion.Version maxOss;
      public final boolean allDSE;
      public final UUID schemaVersion;
      public final boolean schemaAgreement;

      public ClusterVersionInfo(ProductVersion.Version minDse, ProductVersion.Version maxDse, ProductVersion.Version minOss, ProductVersion.Version maxOss, boolean allDSE, UUID schemaVersion, boolean schemaAgreement) {
         this.minDse = minDse;
         this.maxDse = maxDse;
         this.minOss = minOss;
         this.maxOss = maxOss;
         this.allDSE = allDSE;
         this.schemaVersion = schemaVersion;
         this.schemaAgreement = schemaAgreement;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            ClusterVersionBarrier.ClusterVersionInfo that = (ClusterVersionBarrier.ClusterVersionInfo)o;
            return Objects.equals(this.minDse, that.minDse) && Objects.equals(this.maxDse, that.maxDse) && Objects.equals(this.minOss, that.minOss) && Objects.equals(this.maxOss, that.maxOss) && this.allDSE == that.allDSE && Objects.equals(this.schemaVersion, that.schemaVersion) && this.schemaAgreement == that.schemaAgreement;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.minDse, this.maxDse, this.minOss, this.maxOss, Boolean.valueOf(this.allDSE), this.schemaVersion, Boolean.valueOf(this.schemaAgreement)});
      }

      public String toString() {
         return "ClusterVersionInfo{minDse=" + this.minDse + ", maxDse=" + this.maxDse + ", minOss=" + this.minOss + ", maxOss=" + this.maxOss + ", allDSE=" + this.allDSE + ", schemaVersion=" + this.schemaVersion + ", schemaAgreement=" + this.schemaAgreement + '}';
      }
   }

   public static final class EndpointInfo {
      public final ProductVersion.Version dseVersion;
      public final ProductVersion.Version cassandraVersion;
      public final UUID schemaVersion;

      public EndpointInfo(ProductVersion.Version dseVersion, ProductVersion.Version cassandraVersion, UUID schemaVersion) {
         this.dseVersion = dseVersion;
         this.cassandraVersion = cassandraVersion;
         this.schemaVersion = schemaVersion;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            ClusterVersionBarrier.EndpointInfo that = (ClusterVersionBarrier.EndpointInfo)o;
            return Objects.equals(this.dseVersion, that.dseVersion) && Objects.equals(this.cassandraVersion, that.cassandraVersion) && Objects.equals(this.schemaVersion, that.schemaVersion);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.cassandraVersion, this.schemaVersion});
      }

      public String toString() {
         return "EndpointInfo{dseVersion=" + this.dseVersion + ", cassandraVersion=" + this.cassandraVersion + ", schemaVersion=" + this.schemaVersion + '}';
      }
   }
}
