package com.datastax.bdp.plugin;

import java.net.MalformedURLException;

public interface InsightsConfigPluginMXBean {
   void refreshConfig();

   void setMode(String var1);

   String getMode();

   void setConfigRefreshIntervalInSeconds(Integer var1);

   Integer getConfigRefreshIntervalInSeconds();

   void setInsightsUrl(String var1) throws MalformedURLException;

   String getInsightsUrl();

   void setInsightsUploadIntervalInSeconds(Integer var1);

   Integer getInsightsUploadIntervalInSeconds();

   void setMetricSamplingIntervalInSeconds(Integer var1);

   Integer getMetricSamplingIntervalInSeconds();

   void setDataDirMaxSizeInMb(Integer var1);

   Integer getDataDirMaxSizeInMb();

   void setProxyUrl(String var1) throws MalformedURLException;

   String getProxyUrl();

   void setProxyType(String var1);

   String getProxyType();

   void setProxyAuthentication(String var1);

   String getProxyAuthentication();

   void setNodeSystemInformationReportPeriod(String var1);

   String getNodeSystemInformationReportPeriod();

   String getCurrentConfig();

   String getFilteringRules();

   void removeAllFilteringRules();

   void removeFilteringRule(String var1);

   void addFilteringRule(String var1);

   String getInsightsToken();
}
