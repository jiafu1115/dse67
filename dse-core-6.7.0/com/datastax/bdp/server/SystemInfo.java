package com.datastax.bdp.server;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.driver.core.VersionNumber;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Map.Entry;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemInfo {
   private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);
   public static final String DSE_TEST_MODE = "dse.testmode";
   public static final String LEASE_LEADER = "Leader/master/6.0";
   public static final String HIVE_DEF_META_STORE_KEYSPACE = "HiveMetaStore";
   public static final String DSEFS_DEFAULT_KEYSPACE_NAME = "dsefs";
   public static final String DSE_ANALYTICS_KEYSPACE_NAME = "dse_analytics";
   public static final String ADVREP_DEFAULT_KEYSPACE = "dse_advrep";
   public static final String CFS_DEFAULT_KEYSPACE = "cfs";
   public static final String CFS_ARCHIVE_DEFAULT_KEYSPACE = "cfs_archive";
   public static final String SOLR_ADMIN_DEFAULT_KEYSPACE = "solr_admin";
   public static final Set<String> DSE_SYSTEM_KEYSPACE_NAMES = ImmutableSet.of("dse_security", "HiveMetaStore", "dsefs", "dse_analytics", "dse_system", "dse_leases", new String[]{"dse_advrep", "dse_system_local", "cfs", "cfs_archive", "solr_admin", "dse_insights", "dse_insights_local"});
   public static final Set<String> SYSTEM_KEYSPACES;
   public static final TreeMap<VersionNumber, Integer> hiveMetaStoreVerions;
   public static final Integer nonHiveMetastoreVersion;
   public static final Integer hiveMetaStoreVersion;
   private static final String DSE_PERF_KEYSPACE = "dse_perf";
   private static List<String> internalKeySpaces;

   public SystemInfo() {
   }

   public static long getTotalPhysicalMemorySize() {
      OperatingSystemMXBean osmxBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
      return osmxBean.getTotalPhysicalMemorySize();
   }

   public static long getAvailableProcessors() {
      OperatingSystemMXBean osmxBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
      return (long)osmxBean.getAvailableProcessors();
   }

   public static long getMaxHeapMemory() {
      return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
   }

   public static long getCommittedNonHeapMemory() {
      return ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getCommitted();
   }

   public static long getMaxMemory() {
      return getCommittedNonHeapMemory() + getMaxHeapMemory();
   }

   public static UUID getHostId() {
      return StorageService.instance.getLocalHostUUID();
   }

   public static boolean isSystemLease(String leaseName) {
      return leaseName.equals("Leader/master/6.0");
   }

   public static boolean isTestMode() {
      return System.getProperty("dse.testmode") != null;
   }

   public static int getHiveMetastoreVersion(String dseVersion) {
      if("4.6.0".equals(dseVersion)) {
         return nonHiveMetastoreVersion.intValue();
      } else {
         try {
            VersionNumber version = VersionNumber.parse(dseVersion);
            Entry<VersionNumber, Integer> closeDseVersionEntry = hiveMetaStoreVerions.floorEntry(version);
            return closeDseVersionEntry == null?nonHiveMetastoreVersion.intValue():((Integer)closeDseVersionEntry.getValue()).intValue();
         } catch (IllegalArgumentException var3) {
            logger.error("Wrong dse version : " + dseVersion, var3);
            return nonHiveMetastoreVersion.intValue();
         }
      }
   }

   public static boolean isInternalKeyspace(String ks) {
      return internalKeySpaces.contains(ks);
   }

   static {
      SYSTEM_KEYSPACES = (new Builder()).addAll(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES).addAll(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES).addAll(DSE_SYSTEM_KEYSPACE_NAMES).add("system_virtual_schema").build();
      hiveMetaStoreVerions = new TreeMap<VersionNumber, Integer>() {
         {
            this.put(VersionNumber.parse("4.5.5"), Integer.valueOf(1));
            this.put(VersionNumber.parse("5.1.0"), Integer.valueOf(2));
         }
      };
      nonHiveMetastoreVersion = Integer.valueOf(-1);
      hiveMetaStoreVersion = Integer.valueOf(getHiveMetastoreVersion(ProductVersion.getDSEVersionString()));
      Set<String> systemKsNames = new HashSet(DSE_SYSTEM_KEYSPACE_NAMES.size() + SYSTEM_KEYSPACES.size());
      systemKsNames.addAll(DSE_SYSTEM_KEYSPACE_NAMES);
      systemKsNames.addAll(SYSTEM_KEYSPACES);
      internalKeySpaces = new ArrayList(systemKsNames);
      internalKeySpaces.add("dse_perf");
   }
}
