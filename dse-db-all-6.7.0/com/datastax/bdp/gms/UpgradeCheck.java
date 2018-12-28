package com.datastax.bdp.gms;

import com.datastax.bdp.db.util.ProductVersion;
import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeCheck {
   private static final Logger logger = LoggerFactory.getLogger(UpgradeCheck.class);
   private static final Comparator<ProductVersion.Version> MAJOR_MINOR_COMP = (one, two) -> {
      return ComparisonChain.start().compare(one.major, two.major).compare(one.minor, two.minor).result();
   };

   public UpgradeCheck() {
   }

   public static void failIfRequiredUpgradeIsSkipped(ProductVersion.Version currentDseVersion, ProductVersion.Version lastKnownDseVersion) {
      failIfMajorVersionIsSkippedDuringUpgrade(currentDseVersion, lastKnownDseVersion, ProductVersion.DSE_VERSION_50);
   }

   public static void failIfMajorVersionIsSkippedDuringUpgrade(ProductVersion.Version currentDseVersion, ProductVersion.Version lastKnownDseVersion, ProductVersion.Version majorVersionNotAllowedToBeSkipped) {
      if(null != currentDseVersion && null != lastKnownDseVersion && null != majorVersionNotAllowedToBeSkipped) {
         if(currentDseVersion.compareTo(lastKnownDseVersion) > 0) {
            logger.info(String.format("Upgrading from DSE %s to DSE %s", new Object[]{lastKnownDseVersion.toString(), currentDseVersion.toString()}));
         } else if(currentDseVersion.compareTo(lastKnownDseVersion) < 0) {
            logger.info(String.format("Downgrading from DSE %s to DSE %s", new Object[]{lastKnownDseVersion.toString(), currentDseVersion.toString()}));
         }

         if(MAJOR_MINOR_COMP.compare(currentDseVersion, majorVersionNotAllowedToBeSkipped) > 0 && lastKnownDseVersion.compareTo(majorVersionNotAllowedToBeSkipped) < 0) {
            throw new UpgradingException(String.format("Major version skipped during upgrade. You are trying to upgrade from DSE %s to DSE %s without upgrading to the latest version of DSE %s.%s.x first. Uninstall DSE %s and upgrade to DSE %s.%s.x. See http://docs.datastax.com/en/latest-upgrade/ for more details.", new Object[]{lastKnownDseVersion.toString(), currentDseVersion.toString(), Integer.valueOf(majorVersionNotAllowedToBeSkipped.major), Integer.valueOf(majorVersionNotAllowedToBeSkipped.minor), currentDseVersion.toString(), Integer.valueOf(majorVersionNotAllowedToBeSkipped.major), Integer.valueOf(majorVersionNotAllowedToBeSkipped.minor)}));
         }
      }
   }
}
