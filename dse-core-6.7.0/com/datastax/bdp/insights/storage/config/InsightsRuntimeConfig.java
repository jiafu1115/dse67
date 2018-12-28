package com.datastax.bdp.insights.storage.config;

import com.datastax.bdp.config.InsightsConfig;
import com.datastax.bdp.insights.InsightsMode;
import com.datastax.bdp.insights.collectd.CollectdController;
import com.datastax.bdp.tools.InsightsConfigConstants;
import com.datastax.insights.core.json.JacksonUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.Proxy.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(
   ignoreUnknown = true
)
public class InsightsRuntimeConfig {
   private static final Logger logger = LoggerFactory.getLogger(InsightsRuntimeConfig.class);
   public final InsightsRuntimeConfig.Config config;
   @JsonIgnore
   private final Supplier<Boolean> enabled;

   @JsonCreator
   public InsightsRuntimeConfig(@JsonProperty("config") InsightsRuntimeConfig.Config config) {
      this.enabled = Suppliers.memoizeWithExpiration(this::isEnabledAndInstalled, 1L, TimeUnit.HOURS);
      this.config = config;
   }

   @JsonIgnore
   private boolean isEnabledAndInstalled() {
      boolean enabled = this.config != null && !InsightsMode.DISABLED.name().equalsIgnoreCase(this.config.mode) && ((CollectdController)CollectdController.instance.get()).findCollectd().isPresent();
      if(FBUtilities.isLinux && enabled && !Epoll.isAvailable()) {
         logger.error("Insights will be disabled because epoll is not available see: https://docs.datastax.com/en/dse-trblshoot/doc/troubleshooting/startupErrorLibaioNotInstalled.html");
         return false;
      } else if(FBUtilities.isMacOSX && enabled && !KQueue.isAvailable()) {
         logger.error("Insights will be disabled because the netty kqueue transport is not available");
         return false;
      } else if(FBUtilities.isWindows && enabled) {
         logger.error("Insights will be disabled because Windows is not supported");
         return false;
      } else {
         return enabled;
      }
   }

   @JsonIgnore
   public boolean isEnabled() {
      return ((Boolean)this.enabled.get()).booleanValue();
   }

   @JsonIgnore
   public boolean isInsightsUploadEnabled() {
      return this.config != null && this.config.isInsightsUploadEnabled();
   }

   @JsonIgnore
   public boolean isInsightsStreamingEnabled() {
      return this.config != null && this.config.isStreamingEnabled();
   }

   @JsonIgnore
   public boolean isWriteToDiskEnabled() {
      return this.config != null && (InsightsMode.ENABLED_WITH_UPLOAD.name().equalsIgnoreCase(this.config.mode) || InsightsMode.ENABLED_WITH_STREAMING.name().equalsIgnoreCase(this.config.mode) || InsightsMode.ENABLED_WITH_LOCAL_STORAGE.name().equalsIgnoreCase(this.config.mode));
   }

   public String toString() {
      return this.config.toString();
   }

   public boolean proxyConfigEquals(Object o) {
      return this.config.proxyConfigEquals(o);
   }

   public Proxy proxyFromConfig() {
      String proxyUrl = this.config.proxy_url;
      if(StringUtils.isEmpty(proxyUrl)) {
         return Proxy.NO_PROXY;
      } else {
         String proxyType = this.config.proxy_type;

         try {
            URL url = new URL(proxyUrl);
            return new Proxy(Type.valueOf(proxyType.toUpperCase()), InetSocketAddress.createUnresolved(url.getHost(), url.getPort()));
         } catch (Throwable var4) {
            logger.error("Unable to get Insights Collector proxy settings:\n proxy type={}\n proxy url ={}\n Collector proxy settings will be ignored", new Object[]{proxyType, proxyUrl, var4});
            return Proxy.NO_PROXY;
         }
      }
   }

   public long metricUpdateGapInSeconds() {
      return (long)this.config.upload_interval_in_seconds.intValue() > InsightsConfigConstants.MAX_METRIC_UPDATE_GAP_IN_SECONDS?InsightsConfigConstants.MAX_METRIC_UPDATE_GAP_IN_SECONDS:(long)this.config.upload_interval_in_seconds.intValue();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         InsightsRuntimeConfig that = (InsightsRuntimeConfig)o;
         return Objects.equal(this.config, that.config);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.config});
   }

   public static InsightsRuntimeConfig.Builder builder() {
      return new InsightsRuntimeConfig.Builder();
   }

   public static InsightsRuntimeConfig.Builder builder(InsightsRuntimeConfig existingConfig) {
      return new InsightsRuntimeConfig.Builder(existingConfig);
   }

   public static class Builder {
      private String mode;
      private Integer config_refresh_interval_in_seconds;
      private String insights_url;
      private Integer insights_upload_interval_in_seconds;
      private Integer metric_sampling_interval_in_seconds;
      private Integer data_dir_max_size_in_mb;
      private String proxy_type;
      private String proxy_url;
      private String proxy_authentication;
      private String node_system_info_report_period;
      private Collection<FilteringRule> filtering_rules;

      private Builder() {
         this.mode = InsightsConfig.DEFAULT_MODE_SETTING;
         this.config_refresh_interval_in_seconds = Integer.valueOf(30);
         this.insights_url = InsightsConfig.DEFAULT_INSIGHTS_URL;
         this.insights_upload_interval_in_seconds = Integer.valueOf(300);
         this.metric_sampling_interval_in_seconds = Integer.valueOf(30);
         this.data_dir_max_size_in_mb = Integer.valueOf(1024);
         this.proxy_type = "http";
         this.proxy_url = "";
         this.proxy_authentication = "";
         this.node_system_info_report_period = "PT1H";
         this.filtering_rules = Collections.EMPTY_SET;
      }

      private Builder(InsightsRuntimeConfig existingConfig) {
         this.mode = InsightsConfig.DEFAULT_MODE_SETTING;
         this.config_refresh_interval_in_seconds = Integer.valueOf(30);
         this.insights_url = InsightsConfig.DEFAULT_INSIGHTS_URL;
         this.insights_upload_interval_in_seconds = Integer.valueOf(300);
         this.metric_sampling_interval_in_seconds = Integer.valueOf(30);
         this.data_dir_max_size_in_mb = Integer.valueOf(1024);
         this.proxy_type = "http";
         this.proxy_url = "";
         this.proxy_authentication = "";
         this.node_system_info_report_period = "PT1H";
         this.filtering_rules = Collections.EMPTY_SET;
         this.mode = existingConfig.config.mode;
         this.config_refresh_interval_in_seconds = existingConfig.config.config_refresh_interval;
         this.insights_url = existingConfig.config.upload_url;
         this.insights_upload_interval_in_seconds = existingConfig.config.upload_interval_in_seconds;
         this.metric_sampling_interval_in_seconds = existingConfig.config.metric_sampling_interval_in_seconds;
         this.data_dir_max_size_in_mb = existingConfig.config.data_dir_max_size_in_mb;
         this.proxy_authentication = existingConfig.config.proxy_authentication;
         this.proxy_url = existingConfig.config.proxy_url;
         this.proxy_type = existingConfig.config.proxy_type;
         this.node_system_info_report_period = existingConfig.config.node_system_info_report_period;
         this.filtering_rules = existingConfig.config.filtering_rules;
      }

      public InsightsRuntimeConfig.Builder withMode(String mode) {
         this.mode = mode;
         return this;
      }

      public InsightsRuntimeConfig.Builder withConfigRefreshIntervalInSeconds(Integer config_refresh_interval_in_seconds) {
         this.config_refresh_interval_in_seconds = config_refresh_interval_in_seconds;
         return this;
      }

      public InsightsRuntimeConfig.Builder withInsightsUrl(String insights_url) {
         this.insights_url = insights_url;
         return this;
      }

      public InsightsRuntimeConfig.Builder withInsightsUploadIntervalInSeconds(Integer insights_upload_interval_in_seconds) {
         this.insights_upload_interval_in_seconds = insights_upload_interval_in_seconds;
         return this;
      }

      public InsightsRuntimeConfig.Builder withDataDirMaxSizeInMb(Integer data_dir_max_size_in_mb) {
         this.data_dir_max_size_in_mb = data_dir_max_size_in_mb;
         return this;
      }

      public InsightsRuntimeConfig.Builder withProxyType(String proxy_type) {
         this.proxy_type = proxy_type;
         return this;
      }

      public InsightsRuntimeConfig.Builder withProxyUrl(String proxy_url) {
         this.proxy_url = proxy_url;
         return this;
      }

      public InsightsRuntimeConfig.Builder withProxyAuthentication(String proxy_authentication) {
         this.proxy_authentication = proxy_authentication;
         return this;
      }

      public InsightsRuntimeConfig.Builder withNodeSystemInfoReportPeriod(String node_system_info_report_period) {
         this.node_system_info_report_period = node_system_info_report_period;
         return this;
      }

      public InsightsRuntimeConfig.Builder withMetricSamplingIntervalInSeconds(Integer metric_sampling_interval_in_seconds) {
         this.metric_sampling_interval_in_seconds = metric_sampling_interval_in_seconds;
         return this;
      }

      public InsightsRuntimeConfig.Builder withFilteringRules(Collection<FilteringRule> filtering_rules) {
         this.filtering_rules = filtering_rules;
         return this;
      }

      public InsightsRuntimeConfig build() {
         return new InsightsRuntimeConfig(new InsightsRuntimeConfig.Config(this.mode, this.config_refresh_interval_in_seconds, this.metric_sampling_interval_in_seconds, this.insights_url, this.insights_upload_interval_in_seconds, this.data_dir_max_size_in_mb, this.proxy_type, this.proxy_url, this.proxy_authentication, this.node_system_info_report_period, this.filtering_rules));
      }
   }

   public static class Config {
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class, InsightsRuntimeConfig.Config.InsightsToolCommandServiceOptionsDisabledView.class})
      @JsonProperty("mode")
      public final String mode;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class})
      @JsonProperty("upload_url")
      public final String upload_url;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class})
      @JsonProperty("upload_interval_in_seconds")
      public final Integer upload_interval_in_seconds;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class, InsightsRuntimeConfig.Config.InsightsToolCommandServiceOptionsDisabledView.class})
      @JsonProperty("data_dir_max_size_in_mb")
      public final Integer data_dir_max_size_in_mb;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class})
      @JsonProperty("proxy_type")
      public final String proxy_type;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class})
      @JsonProperty("proxy_url")
      public final String proxy_url;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class})
      @JsonProperty("proxy_authentication")
      public final String proxy_authentication;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class, InsightsRuntimeConfig.Config.InsightsToolCommandServiceOptionsDisabledView.class})
      @JsonProperty("node_system_info_report_period")
      public final String node_system_info_report_period;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class, InsightsRuntimeConfig.Config.InsightsToolCommandServiceOptionsDisabledView.class})
      @JsonProperty("config_refresh_interval_in_seconds")
      public final Integer config_refresh_interval;
      @JsonView({InsightsRuntimeConfig.Config.InsightsToolCommandView.class, InsightsRuntimeConfig.Config.InsightsToolCommandServiceOptionsDisabledView.class})
      @JsonProperty("metric_sampling_interval_in_seconds")
      public final Integer metric_sampling_interval_in_seconds;
      @JsonProperty("filtering_rules")
      public final Collection<FilteringRule> filtering_rules;

      public boolean proxyConfigEquals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            InsightsRuntimeConfig.Config config = (InsightsRuntimeConfig.Config)o;
            return Objects.equal(this.proxy_type, config.proxy_type) && Objects.equal(this.proxy_url, config.proxy_url) && Objects.equal(this.proxy_authentication, config.proxy_authentication);
         } else {
            return false;
         }
      }

      @JsonCreator
      public Config(@JsonProperty("mode") String mode, @JsonProperty("config_refresh_interval_in_seconds") Integer config_refresh_interval_in_seconds, @JsonProperty("metric_sampling_interval_in_seconds") Integer metric_sampling_interval_in_seconds, @JsonProperty("upload_url") String upload_url, @JsonProperty("upload_interval_in_seconds") Integer upload_interval_in_seconds, @JsonProperty("data_dir_max_size_in_mb") Integer data_dir_max_size_in_mb, @JsonProperty("proxy_type") String proxy_type, @JsonProperty("proxy_url") String proxy_url, @JsonProperty("proxy_authentication") String proxy_authentication, @JsonProperty("node_system_info_report_period") String node_system_info_report_period, @JsonProperty("filtering_rules") Collection<FilteringRule> filtering_rules) {
         this.mode = mode;
         this.config_refresh_interval = config_refresh_interval_in_seconds;
         this.metric_sampling_interval_in_seconds = metric_sampling_interval_in_seconds;
         this.upload_url = upload_url;
         this.upload_interval_in_seconds = upload_interval_in_seconds;
         this.data_dir_max_size_in_mb = data_dir_max_size_in_mb;
         this.proxy_type = proxy_type;
         this.proxy_url = proxy_url;
         this.proxy_authentication = proxy_authentication;
         this.node_system_info_report_period = node_system_info_report_period;
         this.filtering_rules = filtering_rules;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            InsightsRuntimeConfig.Config config = (InsightsRuntimeConfig.Config)o;
            return Objects.equal(this.mode, config.mode) && Objects.equal(this.upload_url, config.upload_url) && Objects.equal(this.upload_interval_in_seconds, config.upload_interval_in_seconds) && Objects.equal(this.metric_sampling_interval_in_seconds, config.metric_sampling_interval_in_seconds) && Objects.equal(this.data_dir_max_size_in_mb, config.data_dir_max_size_in_mb) && Objects.equal(this.proxy_type, config.proxy_type) && Objects.equal(this.proxy_url, config.proxy_url) && Objects.equal(this.proxy_authentication, config.proxy_authentication) && Objects.equal(this.node_system_info_report_period, config.node_system_info_report_period) && Objects.equal(this.config_refresh_interval, config.config_refresh_interval) && Objects.equal(this.filtering_rules, config.filtering_rules);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hashCode(new Object[]{this.mode, this.upload_url, this.upload_interval_in_seconds, this.metric_sampling_interval_in_seconds, this.data_dir_max_size_in_mb, this.proxy_type, this.proxy_url, this.proxy_authentication, this.node_system_info_report_period, this.config_refresh_interval, this.filtering_rules});
      }

      public String toString() {
         try {
            return JacksonUtil.writeValueAsString(this);
         } catch (Exception var2) {
            throw new RuntimeException("Error rendering insights config as JSON");
         }
      }

      public String displayString() {
         try {
            ObjectMapper mapper = (new ObjectMapper()).configure(SerializationFeature.INDENT_OUTPUT, true);
            return this.shouldShowServiceOptions()?mapper.disable(new MapperFeature[]{MapperFeature.DEFAULT_VIEW_INCLUSION}).writerWithView(InsightsRuntimeConfig.Config.InsightsToolCommandView.class).writeValueAsString(this):mapper.disable(new MapperFeature[]{MapperFeature.DEFAULT_VIEW_INCLUSION}).writerWithView(InsightsRuntimeConfig.Config.InsightsToolCommandServiceOptionsDisabledView.class).writeValueAsString(this);
         } catch (Exception var2) {
            throw new RuntimeException("Error rendering insights config as JSON");
         }
      }

      private boolean shouldShowServiceOptions() {
         return ((Boolean)InsightsConfig.insightsServiceOptionsEnabled.get()).booleanValue();
      }

      boolean isInsightsUploadEnabled() {
         return InsightsMode.ENABLED_WITH_UPLOAD.name().equalsIgnoreCase(this.mode) || InsightsMode.ENABLED_WITH_STREAMING.name().equalsIgnoreCase(this.mode);
      }

      boolean isStreamingEnabled() {
         return InsightsMode.ENABLED_WITH_STREAMING.name().equalsIgnoreCase(this.mode);
      }

      private static final class InsightsToolCommandServiceOptionsDisabledView {
         private InsightsToolCommandServiceOptionsDisabledView() {
         }
      }

      private static final class InsightsToolCommandView {
         private InsightsToolCommandView() {
         }
      }
   }
}
