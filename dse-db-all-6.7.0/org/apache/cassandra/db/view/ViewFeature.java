package org.apache.cassandra.db.view;

import com.datastax.bdp.db.upgrade.ClusterVersionBarrier;
import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;

public final class ViewFeature {
   private static final String ALLOW_MULTIPLE_REGULAR_BASE_COLUMNS_IN_VIEW_PK = "dse.mv.allow_multiple_regular_base_columns_in_view_pk";
   private static volatile boolean canCreateNewFormatViews = true;

   public ViewFeature() {
   }

   public static boolean allowMultipleRegularBaseColumnsInViewPrimaryKey() {
      return PropertyConfiguration.getBoolean("dse.mv.allow_multiple_regular_base_columns_in_view_pk", false);
   }

   @VisibleForTesting
   public static void setAllowMultipleRegularBaseColumnInViewPrimaryKey(boolean enabled) {
      System.setProperty("dse.mv.allow_multiple_regular_base_columns_in_view_pk", Boolean.toString(enabled));
   }

   @VisibleForTesting
   public static void clearAllowMultipleRegularBaseColumnInViewPrimaryKey() {
      System.clearProperty("dse.mv.allow_multiple_regular_base_columns_in_view_pk");
   }

   public static boolean useNewMaterializedView() {
      return !PropertyConfiguration.getBoolean("cassandra.mv.create_legacy_schema", false);
   }

   public static void validateViewVersion() {
      if(!canCreateNewFormatViews) {
         throw new InvalidRequestException("All nodes in the cluster must be on DSE 6.7.0 or newer in order to create Materialized Views using the new format.");
      }
   }

   static {
      Gossiper.instance.clusterVersionBarrier.register((versionInfo) -> {
         canCreateNewFormatViews = versionInfo.allDSE && ProductVersion.DSE_VERSION_67.compareTo(versionInfo.minDse) <= 0;
      });
   }
}
