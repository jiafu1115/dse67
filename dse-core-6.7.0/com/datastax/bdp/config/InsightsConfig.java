package com.datastax.bdp.config;

import com.datastax.bdp.insights.InsightsMode;
import com.datastax.bdp.insights.storage.config.FilteringRule;
import com.google.common.collect.Sets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.SystemUtils;

public class InsightsConfig extends DseConfigYamlLoader {
   public static final String MODE_SYSTEM_PROPERTY_OVERRIDE = String.format("insights.default_%s", new Object[]{"mode"});
   public static final boolean DEFAULT_INSIGHTS_SERVICE_OPTIONS_ENABLED = false;
   public static final String DEFAULT_MODE_SETTING;
   public static final String DEFAULT_INSIGHTS_URL;
   public static final int MIN_INSIGHTS_UPLOAD_INTERVAL_IN_SECONDS = 30;
   public static final int DEFAULT_INSIGHTS_UPLOAD_INTERVAL_IN_SECONDS = 300;
   public static final int DEFAULT_METRICS_SAMPLING_INTERVAL_IN_SECONDS = 30;
   public static final String DEFAULT_PROXY_TYPE = "http";
   public static final String DEFAULT_PROXY_URL = "";
   public static final String DEFAULT_PROXY_CUSTOM_AUTH = "";
   public static final String DEFAULT_DATA_DIRECTORY;
   public static final int DEFAULT_DATA_DIR_MAX_SIZE_IN_MB = 1024;
   public static final String DEFAULT_NODE_SYSTEM_INFO_REPORT_PERIOD = "PT1H";
   public static final int DEFAULT_CONFIG_REFRESH_INTERVAL = 30;
   public static final String DEFAULT_SYSTEM_METRICS_FILTER_REGEX = "org\\.apache\\.cassandra\\.metrics\\.(keyspace|table).*(system_(?!auth)|system\\.(?!(paxos|batches))|dse_|solr_admin|system$).*";
   public static final String DEFAULT_TPC_PER_CORE_FILTER_REGEX = "org\\.apache\\.cassandra\\.metrics\\.thread_pools\\.[^\\.]+.[^\\.]+\\.tpc_\\d+.*$";
   public static final FilteringRule DEFAULT_SYSTEM_TABLE_FILTERING_RULE;
   public static final FilteringRule DEFAULT_TPC_PER_CORE_FILTERING_RULE;
   public static final Collection<FilteringRule> DEFAULT_CONFIG_FILTERING_RULES;
   public static final String DSE_HOME;
   public static final String DEFAULT_LOG_DIR;
   public static final String DEFAULT_COLLECTD_ROOT;
   public static final String DEFAULT_CACERT_FILE;
   public static final ConfigUtil.StringParamResolver dataDirectory;
   public static final ConfigUtil.StringParamResolver collectdRoot;
   public static final ConfigUtil.StringParamResolver logDirectory;
   public static final ConfigUtil.BooleanParamResolver insightsServiceOptionsEnabled;

   public InsightsConfig() {
   }

   static {
      DEFAULT_MODE_SETTING = System.getProperty(MODE_SYSTEM_PROPERTY_OVERRIDE, InsightsMode.ENABLED_WITH_LOCAL_STORAGE.name());
      DEFAULT_INSIGHTS_URL = System.getProperty(String.format("insights.default_%s", new Object[]{"upload_url"}), "https://insights.datastax.com");
      DEFAULT_DATA_DIRECTORY = DatabaseDescriptor.isDaemonInitialized()?Paths.get(DatabaseDescriptor.getCommitLogLocation(), new String[]{"..", "insights_data"}).normalize().toFile().getAbsolutePath():"/var/lib/cassandra/insights_data";
      DEFAULT_SYSTEM_TABLE_FILTERING_RULE = new FilteringRule("deny", "org\\.apache\\.cassandra\\.metrics\\.(keyspace|table).*(system_(?!auth)|system\\.(?!(paxos|batches))|dse_|solr_admin|system$).*", "global");
      DEFAULT_TPC_PER_CORE_FILTERING_RULE = new FilteringRule("deny", "org\\.apache\\.cassandra\\.metrics\\.thread_pools\\.[^\\.]+.[^\\.]+\\.tpc_\\d+.*$", "global");
      DEFAULT_CONFIG_FILTERING_RULES = Sets.newHashSet(new FilteringRule[]{DEFAULT_SYSTEM_TABLE_FILTERING_RULE, DEFAULT_TPC_PER_CORE_FILTERING_RULE});
      DSE_HOME = System.getenv("DSE_HOME") == null?"":System.getenv("DSE_HOME") + "/";
      DEFAULT_LOG_DIR = System.getProperty("cassandra.logdir", System.getProperty("dse.collectd.logdir", "/tmp"));
      if(SystemUtils.IS_OS_MAC_OSX) {
         DEFAULT_COLLECTD_ROOT = "/usr/local/opt/insights-collectd/";
      } else if(Files.isReadable(Paths.get(DSE_HOME, new String[]{"collectd"}))) {
         DEFAULT_COLLECTD_ROOT = DSE_HOME + "collectd/";
      } else {
         DEFAULT_COLLECTD_ROOT = DSE_HOME + "resources/dse/collectd/";
      }

      DEFAULT_CACERT_FILE = DEFAULT_COLLECTD_ROOT + "etc/ssl/certs/ca-certificates.crt";
      dataDirectory = new ConfigUtil.StringParamResolver("insights.data_dir", DEFAULT_DATA_DIRECTORY);
      collectdRoot = new ConfigUtil.StringParamResolver("insights.collectd_root", DEFAULT_COLLECTD_ROOT);
      logDirectory = new ConfigUtil.StringParamResolver("insights.log_dir", DEFAULT_LOG_DIR);
      insightsServiceOptionsEnabled = new ConfigUtil.BooleanParamResolver("insights.service_options_enabled", Boolean.valueOf(false));

      try {
         dataDirectory.withRawParam(config.insights_options.data_dir).check();
         collectdRoot.withRawParam(config.insights_options.collectd_root).check();
         logDirectory.withRawParam(config.insights_options.log_dir).check();
         insightsServiceOptionsEnabled.withRawParam(Boolean.valueOf(config.insights_options.service_options_enabled)).check();
         enableResolvers(InsightsConfig.class);
      } catch (ConfigurationException var1) {
         throw new ExceptionInInitializerError(var1);
      }
   }

   public static final class Names {
      public static final String DATA_DIR = "insights.data_dir";
      public static final String COLLECTD_ROOT = "insights.collectd_root";
      public static final String LOG_DIR = "insights.log_dir";
      public static final String INSIGHTS_SERVICE_OPTIONS_ENABLED = "insights.service_options_enabled";

      public Names() {
      }
   }
}
