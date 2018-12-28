package com.datastax.bdp.insights.storage.config;

import com.datastax.bdp.insights.InsightsMode;
import com.datastax.bdp.insights.storage.credentials.DseInsightsTokenStore;
import com.datastax.bdp.insights.storage.schema.InsightsConfigChangeListener;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.plugin.InsightsConfigPluginMXBean;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.util.MapBuilder;
import com.datastax.insights.core.json.JacksonUtil;
import com.datastax.insights.core.json.JacksonUtil.JacksonUtilException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.Proxy.Type;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class InsightsRuntimeConfigManager implements InsightsConfigPluginMXBean {
   private static final Logger logger = LoggerFactory.getLogger(InsightsRuntimeConfigManager.class);
   private final AtomicReference<InsightsRuntimeConfig> cachedConfig;
   private final InsightsRuntimeConfigAccess access;
   private ScheduledFuture<?> configMonitoringTask;
   private Set<InsightsConfigChangeListener> configChangeListeners = Sets.newConcurrentHashSet();
   private final ThreadPoolPlugin threadPoolPlugin;
   private final DseInsightsTokenStore tokenStore;

   @Inject
   public InsightsRuntimeConfigManager(InsightsRuntimeConfigAccess access, ThreadPoolPlugin threadPoolPlugin, DseInsightsTokenStore tokenStore) {
      this.access = access;
      this.cachedConfig = new AtomicReference((Object)null);
      this.threadPoolPlugin = threadPoolPlugin;
      this.tokenStore = tokenStore;
   }

   public void start() {
      this.populateCachedConfigWithLocalConfig();
      this.startMonitoringForGlobalConfigChanges();
      JMX.registerMBean(this, JMX.Type.INSIGHTS, this.jmxCoordinates());
   }

   private void populateCachedConfigWithLocalConfig() {
      InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights_local");
      if(runtimeConfig == null) {
         this.access.createConfigDefaultsIfNecessary("dse_insights_local");
      }

      this.cachedConfig.set(this.access.selectConfigValue("dse_insights_local"));
   }

   public void stop() {
      this.stopMonitoringForConfigChanges();
      JMX.unregisterMBean(JMX.Type.INSIGHTS, this.jmxCoordinates());
   }

   private void startMonitoringForGlobalConfigChanges() {
      this.configMonitoringTask = this.threadPoolPlugin.scheduleAtFixedRate(this::refreshConfig, (long)((InsightsRuntimeConfig)this.cachedConfig.get()).config.config_refresh_interval.intValue(), (long)((InsightsRuntimeConfig)this.cachedConfig.get()).config.config_refresh_interval.intValue(), TimeUnit.SECONDS);
   }

   private void stopMonitoringForConfigChanges() {
      if(this.configMonitoringTask != null) {
         this.configMonitoringTask.cancel(true);
         this.configMonitoringTask = null;
      }

   }

   public void refreshConfig() {
      try {
         InsightsRuntimeConfig previousConfig = (InsightsRuntimeConfig)this.cachedConfig.get();
         boolean localConfigChanged = this.copyGlobalConfigToLocalIfNecessary();
         if(localConfigChanged) {
            InsightsRuntimeConfig maybeNewConfig = (InsightsRuntimeConfig)this.cachedConfig.get();
            if(maybeNewConfig != null && !maybeNewConfig.equals(previousConfig)) {
               this.notifyConfigChangeListeners(maybeNewConfig, previousConfig);
               if(previousConfig == null || maybeNewConfig.config.config_refresh_interval != null && !Objects.equals(previousConfig.config.config_refresh_interval, maybeNewConfig.config.config_refresh_interval)) {
                  this.stopMonitoringForConfigChanges();
                  this.startMonitoringForGlobalConfigChanges();
               }
            }
         }
      } catch (Exception var4) {
         logger.error("Error encountered while checking for insights configuration changes", var4);
      }

   }

   private void notifyConfigChangeListeners(InsightsRuntimeConfig newConfig, InsightsRuntimeConfig previousConfig) {
      this.configChangeListeners.forEach((configChangeListener) -> {
         configChangeListener.onConfigChanged(previousConfig, newConfig);
      });
   }

   public void addConfigChangeListener(InsightsConfigChangeListener configChangeListener) {
      this.configChangeListeners.add(configChangeListener);
   }

   private boolean copyGlobalConfigToLocalIfNecessary() {
      boolean localConfigChanged = false;
      InsightsRuntimeConfig globalRuntimeConfig = null;

      try {
         globalRuntimeConfig = this.access.selectConfigValue("dse_insights");
         if(globalRuntimeConfig == null) {
            return false;
         }
      } catch (Exception var4) {
         logger.debug("Could not access the global insights runtime configuration");
      }

      InsightsRuntimeConfig localRuntimeConfig = this.access.selectConfigValue("dse_insights_local");
      if(localRuntimeConfig == null) {
         this.access.createConfigDefaultsIfNecessary("dse_insights_local");
         localRuntimeConfig = this.access.selectConfigValue("dse_insights_local");
      }

      if(localRuntimeConfig == null) {
         throw new IllegalStateException("Local insights runtime configuration does not exist");
      } else {
         if(globalRuntimeConfig != null && !globalRuntimeConfig.equals(localRuntimeConfig)) {
            this.access.updateConfig(globalRuntimeConfig, "dse_insights_local");
            localConfigChanged = true;
            localRuntimeConfig = globalRuntimeConfig;
         }

         this.cachedConfig.set(localRuntimeConfig);
         return localConfigChanged;
      }
   }

   public String getMode() {
      return this.getRuntimeConfig().config.mode;
   }

   public void setMode(String mode) {
      this.configureAndRefresh(() -> {
         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;
         Preconditions.checkNotNull(runtimeConfig);
         if(StringUtils.isEmpty(mode)) {
            throw new IllegalArgumentException("Mode cannot be an empty value");
         } else {
            if(!mode.equalsIgnoreCase(runtimeConfig.config.mode)) {
               try {
                  InsightsMode.valueOf(mode.toUpperCase());
               } catch (IllegalArgumentException var4) {
                  throw new IllegalArgumentException(String.format("Invalid mode %s specified.  Valid modes are: %s", new Object[]{mode, StringUtils.join(InsightsMode.values(), ", ")}));
               }

               runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withMode(mode).build();
               this.access.updateConfig(runtimeConfig, "dse_insights");
            }

         }
      });
   }

   public void setConfigRefreshIntervalInSeconds(Integer newRefreshInterval) {
      this.configureAndRefresh(() -> {
         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         if(!newRefreshInterval.equals(runtimeConfig.config.config_refresh_interval)) {
            if(newRefreshInterval.intValue() < 30) {
               throw new IllegalArgumentException("Config refresh interval should be at least 30 seconds");
            }

            runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withConfigRefreshIntervalInSeconds(newRefreshInterval).build();
            this.access.updateConfig(runtimeConfig, "dse_insights");
         }

      });
   }

   public Integer getConfigRefreshIntervalInSeconds() {
      return this.getRuntimeConfig().config.config_refresh_interval;
   }

   public InsightsRuntimeConfig getRuntimeConfig() {
      InsightsRuntimeConfig config = (InsightsRuntimeConfig)this.cachedConfig.get();
      if(config == null) {
         this.populateCachedConfigWithLocalConfig();
      }

      config = (InsightsRuntimeConfig)this.cachedConfig.get();

      assert config != null;

      return config;
   }

   public void setInsightsUrl(String insightsUrl) {
      this.configureAndRefresh(() -> {
         try {
            new URL(insightsUrl);
         } catch (MalformedURLException var3) {
            throw new IllegalArgumentException(String.format("Invalid URL: %s", new Object[]{var3.getMessage()}));
         }

         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withInsightsUrl(insightsUrl).build();
         this.access.updateConfig(runtimeConfig, "dse_insights");
      });
   }

   public String getInsightsUrl() {
      return this.getRuntimeConfig().config.upload_url;
   }

   public void setInsightsUploadIntervalInSeconds(Integer seconds) {
      this.configureAndRefresh(() -> {
         if(seconds != null && seconds.intValue() >= 30) {
            InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
            runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

            assert runtimeConfig != null;

            runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withInsightsUploadIntervalInSeconds(seconds).build();
            this.access.updateConfig(runtimeConfig, "dse_insights");
         } else {
            throw new IllegalArgumentException("Upload interval in seconds must be at least 30 seconds");
         }
      });
   }

   public Integer getInsightsUploadIntervalInSeconds() {
      return this.getRuntimeConfig().config.upload_interval_in_seconds;
   }

   public void setMetricSamplingIntervalInSeconds(Integer seconds) {
      this.configureAndRefresh(() -> {
         if(seconds != null && seconds.intValue() >= 1) {
            InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
            runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

            assert runtimeConfig != null;

            runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withMetricSamplingIntervalInSeconds(seconds).build();
            this.access.updateConfig(runtimeConfig, "dse_insights");
         } else {
            throw new IllegalArgumentException("Metric sampling interval in seconds must be at least 1 second");
         }
      });
   }

   public Integer getMetricSamplingIntervalInSeconds() {
      return this.getRuntimeConfig().config.metric_sampling_interval_in_seconds;
   }

   public void setDataDirMaxSizeInMb(Integer maxSizeInMb) {
      this.configureAndRefresh(() -> {
         if(maxSizeInMb == null) {
            throw new IllegalArgumentException("Cannot set null max data dir size in mb");
         } else if(maxSizeInMb.intValue() < 1) {
            throw new IllegalArgumentException("Data dir max size in mb must be at least 1mb, tohave faster rollovers consider lowering the rollover interval in seconds");
         } else {
            InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
            runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

            assert runtimeConfig != null;

            runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withDataDirMaxSizeInMb(maxSizeInMb).build();
            this.access.updateConfig(runtimeConfig, "dse_insights");
         }
      });
   }

   public Integer getDataDirMaxSizeInMb() {
      return this.getRuntimeConfig().config.data_dir_max_size_in_mb;
   }

   public void setProxyUrl(String proxyUrl) {
      this.configureAndRefresh(() -> {
         try {
            new URL(proxyUrl);
         } catch (MalformedURLException var3) {
            throw new IllegalArgumentException(String.format("Invalid URL: %s", new Object[]{var3.getMessage()}));
         }

         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withProxyUrl(proxyUrl).build();
         this.access.updateConfig(runtimeConfig, "dse_insights");
      });
   }

   public String getProxyUrl() {
      return this.getRuntimeConfig().config.proxy_url;
   }

   public void setProxyType(String proxyType) {
      this.configureAndRefresh(() -> {
         if(!StringUtils.isEmpty(proxyType)) {
            try {
               Type.valueOf(proxyType.toUpperCase());
            } catch (Exception var3) {
               throw new IllegalArgumentException(String.format("%s is not a valid proxy type.  Please specify http, socks, or direct", new Object[]{proxyType}));
            }
         }

         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withProxyType(proxyType != null?proxyType:"").build();
         this.access.updateConfig(runtimeConfig, "dse_insights");
      });
   }

   public String getProxyType() {
      return this.getRuntimeConfig().config.proxy_type;
   }

   public void setProxyAuthentication(String proxyAuthentication) {
      this.configureAndRefresh(() -> {
         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withProxyAuthentication(proxyAuthentication).build();
         this.access.updateConfig(runtimeConfig, "dse_insights");
      });
   }

   public String getProxyAuthentication() {
      return this.getRuntimeConfig().config.proxy_authentication;
   }

   public void setNodeSystemInformationReportPeriod(String nodeSystemInformationReportPeriod) {
      this.configureAndRefresh(() -> {
         try {
            Duration.parse(nodeSystemInformationReportPeriod);
         } catch (Exception var3) {
            throw new IllegalArgumentException(String.format("Invalid ISO-8601 duration: %s", new Object[]{nodeSystemInformationReportPeriod}));
         }

         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withNodeSystemInfoReportPeriod(nodeSystemInformationReportPeriod).build();
         this.access.updateConfig(runtimeConfig, "dse_insights");
      });
   }

   public String getNodeSystemInformationReportPeriod() {
      return this.getRuntimeConfig().config.node_system_info_report_period;
   }

   public String getCurrentConfig() {
      InsightsRuntimeConfig runtimeConfig = this.getRuntimeConfig();
      if(runtimeConfig == null) {
         throw new IllegalStateException("Cannot find existing Insights Configuration");
      } else {
         return runtimeConfig.config.displayString();
      }
   }

   public String getFilteringRules() {
      InsightsRuntimeConfig runtimeConfig = this.getRuntimeConfig();
      if(runtimeConfig == null) {
         throw new IllegalStateException("Cannot find existing Insights Configuration");
      } else {
         try {
            return JacksonUtil.prettyPrint(runtimeConfig.config.filtering_rules);
         } catch (JacksonUtilException var3) {
            throw new IllegalArgumentException("Cannot serialize filtering rules to json", var3);
         }
      }
   }

   public void addFilteringRule(String json) {
      this.configureAndRefresh(() -> {
         try {
            FilteringRule rule = (FilteringRule)JacksonUtil.readValue(json, FilteringRule.class);
            InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
            runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

            assert runtimeConfig != null;

            runtimeConfig.config.filtering_rules.forEach((filteringRule) -> {
               if(filteringRule.patternStr.equals(rule.patternStr)) {
                  throw new DuplicateFilteringRuleError(filteringRule);
               }
            });
            runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withFilteringRules(Sets.union(Sets.newHashSet(runtimeConfig.config.filtering_rules), Sets.newHashSet(new FilteringRule[]{rule}))).build();
            this.access.updateConfig(runtimeConfig, "dse_insights");
         } catch (JacksonUtilException var4) {
            throw new IllegalArgumentException("Invalid filtering rule format", var4);
         }
      });
   }

   public void removeFilteringRule(String json) {
      this.configureAndRefresh(() -> {
         try {
            FilteringRule rule = (FilteringRule)JacksonUtil.readValue(json, FilteringRule.class);
            InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
            runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

            assert runtimeConfig != null;

            Set<FilteringRule> copy = Sets.newHashSet(runtimeConfig.config.filtering_rules);
            if(!copy.remove(rule)) {
               throw new IllegalArgumentException("No matching rule found in existing rule set for " + json);
            } else {
               runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withFilteringRules(copy).build();
               this.access.updateConfig(runtimeConfig, "dse_insights");
            }
         } catch (JacksonUtilException var5) {
            throw new IllegalArgumentException("Invalid filtering rule format", var5);
         }
      });
   }

   public void removeAllFilteringRules() {
      this.configureAndRefresh(() -> {
         InsightsRuntimeConfig runtimeConfig = this.access.selectConfigValue("dse_insights");
         runtimeConfig = runtimeConfig == null?this.access.selectConfigValue("dse_insights_local"):runtimeConfig;

         assert runtimeConfig != null;

         runtimeConfig = InsightsRuntimeConfig.builder(runtimeConfig).withFilteringRules(Collections.EMPTY_SET).build();
         this.access.updateConfig(runtimeConfig, "dse_insights");
      });
   }

   public String getInsightsToken() {
      return (String)this.tokenStore.token().orElse("[token missing.  Check that the upload_url is setup correctly and is reachable from this node]");
   }

   private MapBuilder.ImmutableMap<String, String> jmxCoordinates() {
      return MapBuilder.immutable().withKeys(new String[]{"name"}).withValues(new String[]{"InsightsRuntimeConfig"}).build();
   }

   private void configureAndRefresh(Runnable configureAction) {
      try {
         configureAction.run();
      } finally {
         this.refreshConfig();
      }

   }

   public boolean isEnabled() {
      return this.getRuntimeConfig().isEnabled();
   }
}
