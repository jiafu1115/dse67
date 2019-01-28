package com.datastax.bdp.db.upgrade;

import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.PropertyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class VersionDependentFeature<I extends VersionDependentFeature.VersionDependent> {
   private static final Logger logger = LoggerFactory.getLogger(VersionDependentFeature.class);
   private static final long postDdlChangeCheckMillis = PropertyConfiguration.getLong("dse.versionDependentFeature.postDdlChangeCheckMillis", 1000L);
   private VersionDependentFeature.FeatureStatus status;
   private final I currentImplementation;
   private final I legacyImplementation;
   private final String name;
   private final ProductVersion.Version minimumDseVersion;
   private final ProductVersion.Version minimumOssVersion;
   private final boolean requireDSE;
   private boolean currentInitialized;
   private boolean legacyInitialized;
   private Future<?> ddlFuture;
   private ClusterVersionBarrier clusterVersionBarrier;

   protected VersionDependentFeature(String name, ProductVersion.Version minimumDseVersion, ProductVersion.Version minimumOssVersion, boolean requireDSE, I legacyImplementation, I currentImplementation) {
      this.status = VersionDependentFeature.FeatureStatus.UNKNOWN;
      this.name = name;

      assert minimumDseVersion != null || minimumOssVersion != null;

      this.minimumDseVersion = minimumDseVersion;
      this.minimumOssVersion = minimumOssVersion;
      this.requireDSE = requireDSE;
      this.legacyImplementation = legacyImplementation;
      this.currentImplementation = currentImplementation;
   }

   public static <I extends VersionDependentFeature.VersionDependent> VersionDependentFeature.SchemaUpgradeBuilder<I> newSchemaUpgradeBuilder() {
      return new VersionDependentFeature.SchemaUpgradeBuilder();
   }

   public void setup(ClusterVersionBarrier clusterVersionBarrier) {
      clusterVersionBarrier.register(this::clusterVersionUpdated);
      this.clusterVersionBarrier = clusterVersionBarrier;
   }

   @VisibleForTesting
    public synchronized void clusterVersionUpdated(ClusterVersionBarrier.ClusterVersionInfo versionInfo) {
        logger.trace("clusterVersionUpdated for {}/{}: {}", new Object[]{this.name, this.status, versionInfo});
        boolean minVersionOK = !(this.requireDSE && !versionInfo.allDSE || this.minimumDseVersion != null && this.minimumDseVersion.compareTo(versionInfo.minDse) > 0 || this.minimumOssVersion != null && this.minimumOssVersion.compareTo(versionInfo.minOss) > 0);
        switch (this.status) {
            case UNKNOWN:
            case DEACTIVATED: {
                if (minVersionOK && !this.ddlChangeRequired()) {
                    this.updateStatus(FeatureStatus.ACTIVATED);
                    break;
                }
                if (minVersionOK) {
                    this.updateStatus(FeatureStatus.DEACTIVATED);
                    this.updateStatus(FeatureStatus.ACTIVATING);
                    this.scheduleCallback();
                    break;
                }
                this.updateStatus(FeatureStatus.DEACTIVATED);
                break;
            }
            case ACTIVATING: {
                if (this.ddlChangeRequired()) {
                    if (!versionInfo.schemaAgreement) break;
                    this.maybeScheduleDDL();
                    break;
                }
                if (!versionInfo.schemaAgreement) break;
                this.updateStatus(FeatureStatus.ACTIVATED);
                break;
            }
            case ACTIVATED: {
                if (minVersionOK) break;
                this.updateStatus(FeatureStatus.DEACTIVATED);
            }
        }
    }

   private synchronized void maybeScheduleDDL() {
      if(this.ddlFuture == null) {
         logger.info("Scheduling DDL change for '{}'", this.name);
         this.ddlFuture = ScheduledExecutors.nonPeriodicTasks.submit(() -> {
            try {
               this.executeDDL();
            } finally {
               this.ddlFinished();
               this.scheduleCallback();
            }

         });
      }

   }

   private synchronized void scheduleCallback() {
      ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
         this.clusterVersionUpdated(this.clusterVersionBarrier.currentClusterVersionInfo());
      }, postDdlChangeCheckMillis, TimeUnit.MILLISECONDS);
   }

   private synchronized void ddlFinished() {
      logger.debug("DDL change for '{}' finished", this.name);
      this.ddlFuture = null;
   }

   private synchronized void updateStatus(FeatureStatus newStatus) {
      if (this.status != newStatus) {
         this.status = newStatus;
         logger.debug("New status for '{}': {}", (Object)this.name, (Object)newStatus);
         switch (newStatus) {
            case ACTIVATED: {
               if (!this.currentInitialized) {
                  this.currentImplementation.initialize();
                  this.currentInitialized = true;
               }
               this.onFeatureActivated();
               break;
            }
            case ACTIVATING: {
               this.onFeatureActivating();
               break;
            }
            case DEACTIVATED: {
               if (!this.legacyInitialized) {
                  this.legacyImplementation.initialize();
                  this.legacyInitialized = true;
               }
               this.onFeatureDeactivated();
               break;
            }
            default: {
               throw new RuntimeException("Unknown new status " + (Object)((Object)newStatus));
            }
         }
      }
   }

   public I implementation() {
      return this.status == VersionDependentFeature.FeatureStatus.ACTIVATED?this.currentImplementation:this.legacyImplementation;
   }

   public VersionDependentFeature.FeatureStatus getStatus() {
      return this.status;
   }

   protected abstract boolean ddlChangeRequired();

   protected abstract void executeDDL();

   public abstract void onFeatureDeactivated();

   public abstract void onFeatureActivating();

   public abstract void onFeatureActivated();

   public String toString() {
      return "VersionDependentFeature{name='" + this.name + '\'' + ", minimumDseVersion=" + this.minimumDseVersion + ", minimumOssVersion=" + this.minimumOssVersion + ", status=" + this.status + '}';
   }

   public interface VersionDependent {
      void initialize();
   }

   public static enum FeatureStatus {
      UNKNOWN,
      DEACTIVATED,
      ACTIVATING,
      ACTIVATED;

      private FeatureStatus() {
      }
   }

   public static final class SchemaUpgradeBuilder<I extends VersionDependentFeature.VersionDependent> extends VersionDependentFeature.VersionDependentFeatureBuilder<I, VersionDependentFeature.SchemaUpgradeBuilder<I>> {
      SchemaUpgradeBuilder() {
      }

      public VersionDependentFeature<I> build() {
         return new VersionDependentFeature<I>(this.name, this.minimumDseVersion, this.minimumOssVersion, this.requireDSE, this.legacyImplementation, this.currentImplementation) {
            protected boolean ddlChangeRequired() {
               return SchemaUpgradeBuilder.this.schemaUpgrade.ddlChangeRequired();
            }

            protected void executeDDL() {
               SchemaUpgradeBuilder.this.schemaUpgrade.executeDDL();
            }

            public void onFeatureActivating() {
               SchemaUpgradeBuilder.this.logger.info(SchemaUpgradeBuilder.this.messageActivating);
            }

            public void onFeatureActivated() {
               SchemaUpgradeBuilder.this.logger.info(SchemaUpgradeBuilder.this.messageActivated);
            }

            public void onFeatureDeactivated() {
               SchemaUpgradeBuilder.this.logger.info(SchemaUpgradeBuilder.this.messageDeactivated);
            }
         };
      }
   }

   public abstract static class VersionDependentFeatureBuilder<I extends VersionDependentFeature.VersionDependent, B extends VersionDependentFeature.VersionDependentFeatureBuilder<I, B>> {
      protected String name;
      protected ProductVersion.Version minimumDseVersion;
      protected ProductVersion.Version minimumOssVersion;
      protected boolean requireDSE;
      protected I legacyImplementation;
      protected I currentImplementation;
      protected SchemaUpgrade schemaUpgrade;
      protected Logger logger;
      protected String messageActivating;
      protected String messageActivated;
      protected String messageDeactivated;

      public VersionDependentFeatureBuilder() {
      }

      private B me() {
         return (B)this;
      }

      public B withName(String name) {
         this.name = name;
         return this.me();
      }

      public B withMinimumDseVersion(ProductVersion.Version minimumDseVersion) {
         this.minimumDseVersion = minimumDseVersion;
         return this.me();
      }

      public B withMinimumOssVersion(ProductVersion.Version minimumOssVersion) {
         this.minimumOssVersion = minimumOssVersion;
         return this.me();
      }

      public B withRequireDSE(boolean requireDSE) {
         this.requireDSE = requireDSE;
         return this.me();
      }

      public B withLegacyImplementation(I legacyImplementation) {
         this.legacyImplementation = legacyImplementation;
         return this.me();
      }

      public B withCurrentImplementation(I currentImplementation) {
         this.currentImplementation = currentImplementation;
         return this.me();
      }

      public B withSchemaUpgrade(SchemaUpgrade schemaUpgrade) {
         this.schemaUpgrade = schemaUpgrade;
         return this.me();
      }

      public B withLogger(Logger logger) {
         this.logger = logger;
         return this.me();
      }

      public B withMessageActivating(String messageActivating) {
         this.messageActivating = messageActivating;
         return this.me();
      }

      public B withMessageActivated(String messageActivated) {
         this.messageActivated = messageActivated;
         return this.me();
      }

      public B withMessageDeactivated(String messageDeactivated) {
         this.messageDeactivated = messageDeactivated;
         return this.me();
      }
   }
}
