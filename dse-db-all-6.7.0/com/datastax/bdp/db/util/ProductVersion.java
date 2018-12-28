package com.datastax.bdp.db.util;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductVersion {
   private static final Logger logger = LoggerFactory.getLogger(ProductVersion.class);
   public static final ProductVersion.Version DSE_VERSION_50 = new ProductVersion.Version("5.0.0");
   public static final ProductVersion.Version DSE_VERSION_51 = new ProductVersion.Version("5.1.0");
   public static final ProductVersion.Version DSE_VERSION_60 = new ProductVersion.Version("6.0.0");
   public static final ProductVersion.Version DSE_VERSION_67 = new ProductVersion.Version("6.7.0");
   public static final String VERSION_PROPERTIES_FILE = "com/datastax/bdp/version.properties";
   public static final String DSE_STATE_LOCAL_VERSION = "dse.state.local_version";
   public static final String UNKNOWN_DSE_VERSION = "5.0.0-Unknown-SNAPSHOT";
   public static final String DSE_VERSION_KEY = "dse_version";
   public static final String DSE_FULL_VERSION_KEY = "dse_full_version";
   public static final String RELEASE_VERSION_KEY = "release_version";
   public static final String SOLR_VERSION_KEY = "solr_version";
   public static final String APPENDER_VERSION_KEY = "appender_version";
   public static final String SPARK_VERSION_KEY = "spark_version";
   public static final String SPARK_JOB_SERVER_VERSION_KEY = "spark_job_server_version";
   public static final String VCS_ID = "vcs_id";
   public static final String VCS_BRANCH = "vcs_branch";
   public static final String[] ALL_KEYS = new String[]{"dse_version", "dse_full_version", "release_version", "solr_version", "appender_version", "spark_version", "spark_job_server_version", "vcs_id", "vcs_branch"};

   public ProductVersion() {
   }

   public static String getReleaseVersionString() {
      return ProductVersion.VersionProperties.cassandraVersionString;
   }

   public static ProductVersion.Version getReleaseVersion() {
      return ProductVersion.VersionProperties.cassandraVersion;
   }

   public static String getDSEVersionString() {
      return ProductVersion.VersionProperties.dseVersionString;
   }

   public static ProductVersion.Version getDSEVersion() {
      return ProductVersion.VersionProperties.dseVersion;
   }

   public static String getDSEFullVersionString() {
      return ProductVersion.VersionProperties.dseFullVersionString;
   }

   public static ProductVersion.Version getDSEFullVersion() {
      return ProductVersion.VersionProperties.dseFullVersion;
   }

   public static String getProductVersionString(String other) {
      byte var2 = -1;
      switch(other.hashCode()) {
      case 114832207:
         if(other.equals("dse_version")) {
            var2 = 1;
         }
         break;
      case 280818848:
         if(other.equals("release_version")) {
            var2 = 0;
         }
         break;
      case 1951941393:
         if(other.equals("dse_full_version")) {
            var2 = 2;
         }
      }

      switch(var2) {
      case 0:
         return getReleaseVersionString();
      case 1:
         return getDSEVersionString();
      case 2:
         return getDSEFullVersionString();
      default:
         return ProductVersion.VersionProperties.all.getProperty(other);
      }
   }

   public static final class DseAndOssVersions {
      public final ProductVersion.Version dseVersion;
      public final ProductVersion.Version ossVersion;

      public DseAndOssVersions(ProductVersion.Version dseVersion, ProductVersion.Version ossVersion) {
         this.dseVersion = dseVersion;
         this.ossVersion = ossVersion;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            ProductVersion.DseAndOssVersions that = (ProductVersion.DseAndOssVersions)o;
            return Objects.equals(this.dseVersion, that.dseVersion) && Objects.equals(this.ossVersion, that.ossVersion);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.dseVersion, this.ossVersion});
      }

      public String toString() {
         return (this.dseVersion != null?"DSE-version:" + this.dseVersion:"DSE-version:none") + (this.ossVersion != null?", OSS-version:" + this.ossVersion:", OSS-version:none");
      }
   }

   public static final class Version implements Comparable<ProductVersion.Version> {
      private static final Pattern versionPattern = Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\w+)(?:\\.(\\w+))?)?(-([^+]+))?([+](.+))?");
      private static final Pattern snapshotPattern = Pattern.compile("^(.*)-SNAPSHOT$", 2);
      public static final ProductVersion.Version nullVersion = new ProductVersion.Version("0.0.0");
      private final String versionString;
      public final int major;
      public final int minor;
      public final int patch;
      public final int buildnum;
      public final boolean snapshot;
      private final String[] preRelease;
      private final String[] build;

      public Version(String version) {
         this.versionString = version;
         Matcher snapshotMatcher = snapshotPattern.matcher(version);
         this.snapshot = snapshotMatcher.matches();
         if(this.snapshot) {
            version = snapshotMatcher.group(1);
         }

         Matcher matcher = versionPattern.matcher(version);
         if(!matcher.matches()) {
            throw new IllegalArgumentException("Invalid version value: " + version);
         } else {
            try {
               this.major = Integer.parseInt(matcher.group(1));
               this.minor = Integer.parseInt(matcher.group(2));
               this.patch = matcher.group(3) != null?Integer.parseInt(matcher.group(3)):0;
               this.buildnum = matcher.group(4) != null?Integer.parseInt(matcher.group(4)):0;
               String pr = matcher.group(6);
               String bld = matcher.group(8);
               this.preRelease = pr != null && !pr.isEmpty()?parseIdentifiers(pr):null;
               this.build = bld != null && !bld.isEmpty()?parseIdentifiers(bld):null;
            } catch (NumberFormatException var6) {
               throw new IllegalArgumentException("Invalid version value: " + version, var6);
            }
         }
      }

      public Version(int major, int minor) {
         this(major, minor, 0, 0);
      }

      public Version(int major, int minor, int patch, int buildnum) {
         this.major = major;
         this.minor = minor;
         this.patch = patch;
         this.buildnum = buildnum;
         this.snapshot = false;
         this.preRelease = null;
         this.build = null;
         String s = Integer.toString(major) + '.' + minor;
         if(patch > 0 || buildnum > 0) {
            s = s + "." + patch;
            if(buildnum > 0) {
               s = s + "." + buildnum;
            }
         }

         this.versionString = s;
      }

      private static String[] parseIdentifiers(String str) {
         return StringUtils.split(str, '.');
      }

      public int compareTo(ProductVersion.Version other) {
         if(this.major < other.major) {
            return -1;
         } else if(this.major > other.major) {
            return 1;
         } else if(this.minor < other.minor) {
            return -1;
         } else if(this.minor > other.minor) {
            return 1;
         } else if(this.patch < other.patch) {
            return -1;
         } else if(this.patch > other.patch) {
            return 1;
         } else if(this.buildnum < other.buildnum) {
            return -1;
         } else if(this.buildnum > other.buildnum) {
            return 1;
         } else {
            int c = compareIdentifiers(this.preRelease, other.preRelease, 1);
            return c != 0?c:compareIdentifiers(this.build, other.build, -1);
         }
      }

      private static int compareIdentifiers(String[] ids1, String[] ids2, int defaultPred) {
         if(ids1 == null) {
            return ids2 == null?0:defaultPred;
         } else if(ids2 == null) {
            return -defaultPred;
         } else {
            int min = Math.min(ids1.length, ids2.length);

            for(int i = 0; i < min; ++i) {
               Integer i1 = tryParseInt(ids1[i]);
               Integer i2 = tryParseInt(ids2[i]);
               if(i1 != null) {
                  if(i2 == null || i1.intValue() < i2.intValue()) {
                     return -1;
                  }

                  if(i1.intValue() > i2.intValue()) {
                     return 1;
                  }
               } else {
                  if(i2 != null) {
                     return 1;
                  }

                  int c = ids1[i].compareTo(ids2[i]);
                  if(c != 0) {
                     return c;
                  }
               }
            }

            if(ids1.length < ids2.length) {
               return -1;
            } else if(ids1.length > ids2.length) {
               return 1;
            } else {
               return 0;
            }
         }
      }

      private static Integer tryParseInt(String str) {
         try {
            return Integer.valueOf(str);
         } catch (NumberFormatException var2) {
            return null;
         }
      }

      public boolean sameMajorMinorVersion(ProductVersion.Version other) {
         return this.major == other.major && this.minor == other.minor;
      }

      public boolean isReleaseVersion() {
         return !this.snapshot && this.preRelease == null && this.build == null;
      }

      public ProductVersion.Version asReleaseVersion() {
         return new ProductVersion.Version(this.major, this.minor, this.patch, this.buildnum);
      }

      public boolean equals(Object o) {
         if(!(o instanceof ProductVersion.Version)) {
            return false;
         } else {
            ProductVersion.Version that = (ProductVersion.Version)o;
            return this.major == that.major && this.minor == that.minor && this.patch == that.patch && Arrays.equals(this.preRelease, that.preRelease) && Arrays.equals(this.build, that.build);
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{Integer.valueOf(this.major), Integer.valueOf(this.minor), Integer.valueOf(this.patch), this.preRelease, this.build});
      }

      public String toString() {
         return this.versionString;
      }
   }

   private static final class VersionProperties {
      static final ProductVersion.Version cassandraVersion;
      static final ProductVersion.Version dseVersion;
      static final ProductVersion.Version dseFullVersion;
      static final String cassandraVersionString;
      static final String dseVersionString;
      static final String dseFullVersionString;
      static final Properties all;

      private VersionProperties() {
      }

      static {
         String oss = "5.0.0-Unknown-SNAPSHOT";
         String dse = "5.0.0-Unknown-SNAPSHOT";
         String dseFull = "5.0.0-Unknown-SNAPSHOT";
         Properties props = new Properties();

         try {
            InputStream in = ProductVersion.class.getClassLoader().getResourceAsStream("com/datastax/bdp/version.properties");
            Throwable var5 = null;

            try {
               if(in != null) {
                  props.load(in);
                  oss = props.getProperty("release_version", oss);
                  dse = props.getProperty("dse_version", dse);
                  dseFull = props.getProperty("dse_full_version", dse);
               }
            } catch (Throwable var15) {
               var5 = var15;
               throw var15;
            } finally {
               if(in != null) {
                  if(var5 != null) {
                     try {
                        in.close();
                     } catch (Throwable var14) {
                        var5.addSuppressed(var14);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         } catch (Exception var17) {
            JVMStabilityInspector.inspectThrowable(var17);
            ProductVersion.logger.error("Unable to load version.properties", var17);
         }

         String dseVersionOverride = System.getProperty("dse.state.local_version");
         if(dseVersionOverride != null) {
            dse = dseVersionOverride;
         }

         cassandraVersionString = oss;
         dseVersionString = dse;
         dseFullVersionString = dseFull;
         cassandraVersion = new ProductVersion.Version(oss);
         dseVersion = new ProductVersion.Version(dse);
         dseFullVersion = new ProductVersion.Version(dseFull);
         all = props;
      }
   }
}
