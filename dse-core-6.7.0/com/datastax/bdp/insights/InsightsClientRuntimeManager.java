package com.datastax.bdp.insights;

import com.datastax.bdp.insights.events.InsightsClientStarted;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.datastax.bdp.insights.storage.schema.InsightsConfigChangeListener;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.net.ProxySelector;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class InsightsClientRuntimeManager implements InsightsRuntimeConfigComponent, InsightsConfigChangeListener {
   private static final Logger logger = LoggerFactory.getLogger(InsightsClientRuntimeManager.class);
   private final AtomicBoolean started = new AtomicBoolean(false);
   private final InsightsClient insightsClient;
   private final ThreadPoolPlugin threadPoolPlugin;

   @Inject
   public InsightsClientRuntimeManager(InsightsClient client, ThreadPoolPlugin threadPoolPlugin) {
      this.insightsClient = client;
      this.threadPoolPlugin = threadPoolPlugin;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         logger.debug("Configuration changed to enable insights, starting insights client");

         try {
            this.insightsClient.start();
            this.insightsClient.report(new InsightsClientStarted());
         } catch (Exception var2) {
            logger.error("Error starting insights client", var2);
         }
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         logger.debug("Configuration changed to disable insights, stopping insights client");

         try {
            this.insightsClient.close();
         } catch (IOException var2) {
            logger.warn("Exception closing the insights client", var2);
         }
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.empty();
   }

   public boolean shouldRestart(InsightsRuntimeConfig previousConfig, InsightsRuntimeConfig newConfig) {
      return false;
   }

   public void onConfigChanged(InsightsRuntimeConfig previousConfig, InsightsRuntimeConfig newConfig) {
      logger.debug("Insights configuration has changed. Previous Configuration {}, New Configuration {}", previousConfig, newConfig);
      if(!Objects.equals(previousConfig.config.upload_interval_in_seconds, newConfig.config.upload_interval_in_seconds)) {
         this.insightsClient.reconfigureCollectorUploadIntervalInSeconds(newConfig.config.upload_interval_in_seconds);
      }

      if(!Objects.equals(previousConfig.config.upload_url, newConfig.config.upload_url)) {
         this.insightsClient.reconfigureCollectorUrl(newConfig.config.upload_url);
      }

      if(!Objects.equals(previousConfig.config.data_dir_max_size_in_mb, newConfig.config.data_dir_max_size_in_mb)) {
         this.insightsClient.reconfigureDataDirMaxSizeInMb(newConfig.config.data_dir_max_size_in_mb);
      }

      if(!previousConfig.proxyConfigEquals(newConfig)) {
         this.insightsClient.reconfigureHttpClient(true, newConfig.proxyFromConfig(), (ProxySelector)null, newConfig.config.proxy_authentication);
      }

   }
}
