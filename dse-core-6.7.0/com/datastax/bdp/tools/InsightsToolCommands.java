package com.datastax.bdp.tools;

import com.datastax.bdp.config.InsightsConfig;
import com.datastax.bdp.insights.InsightsMode;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfigAccess;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.plugin.InsightsConfigPluginMXBean;
import com.datastax.bdp.util.DseUtil;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

public class InsightsToolCommands implements ProxySource {
   public InsightsToolCommands() {
   }

   public void makeProxies(NodeJmxProxyPool probe) throws MalformedObjectNameException {
      makeProxy(probe);
   }

   private static InsightsConfigPluginMXBean makeProxy(NodeJmxProxyPool probe) throws MalformedObjectNameException {
      return (InsightsConfigPluginMXBean)probe.makeProxy(JMX.Type.INSIGHTS, "InsightsRuntimeConfig", InsightsConfigPluginMXBean.class);
   }

   public static class ConfigureInsightsCommand extends DseTool.Plugin {
      private static final Options OPTIONS = (new Options()).addOption("show_config", "show_config", false, "Show current Insights configuration").addOption("show_token", "show_token", false, "Show current Insights service provided token for this node").addOption("mode", "mode", true, "Sets the Insights mode").addOption("metric_sampling_interval_in_seconds", "metric_sampling_interval_in_seconds", true, "Change Insights metric sampling interval in seconds").addOption("config_refresh_interval_in_seconds", "config_refresh_interval_in_seconds", true, "Change the frequency insights configuration changes are applied to nodes").addOption("data_dir_max_size_in_mb", "data_dir_max_size_in_mb", true, "Change the maximum size of the insights data directory where insights are stored if storage is enabled").addOption("node_system_info_report_period", "node_system_info_report_period", true, "Change the node system info report period(ISO-8601 duration string)").addOption("upload_url", "upload_url", true, "Change Insights Service URL to upload data to").addOption("upload_interval_in_seconds", "upload_interval_in_seconds", true, "Change upload interval in seconds to the Insights service").addOption("proxy_url", "proxy_url", true, "Change the proxy url for insights uploads").addOption("proxy_authentication", "proxy_authentication", true, "Change the proxy authentication header string").addOption("proxy_type", "proxy_type", true, "Change the proxy type - default http");

      public ConfigureInsightsCommand() {
      }

      public String getName() {
         return "insights_config";
      }

      public boolean isJMX() {
         return true;
      }

      public String getHelp() {
         return "Insights Configuration";
      }

      public String getOptionsHelp() {
         EnumSet<InsightsMode> modes = EnumSet.allOf(InsightsMode.class);
         List<InsightsMode> enabledModes = (List)modes.stream().filter((mode) -> {
            return ((Boolean)InsightsConfig.insightsServiceOptionsEnabled.get()).booleanValue() || !InsightsMode.ENABLED_WITH_STREAMING.equals(mode) && !InsightsMode.ENABLED_WITH_UPLOAD.equals(mode);
         }).collect(Collectors.toList());
         StringBuilder optionsBuilder = new StringBuilder();
         optionsBuilder.append(String.format("--%s <" + StringUtils.join(enabledModes, '|') + "> %s\n", new Object[]{"mode", "Sets the Insights mode"}));
         this.addOption(optionsBuilder, "show_config", "", "Show current Insights configuration", false);
         this.addOption(optionsBuilder, "show_token", "", "Show current Insights service provided token for this node", true);
         this.addOption(optionsBuilder, "upload_url", "<url string>", "Change Insights Service URL to upload data to", true);
         this.addOption(optionsBuilder, "upload_interval_in_seconds", "<int>", "Change upload interval in seconds to the Insights service", true);
         this.addOption(optionsBuilder, "proxy_type", "<http|direct|socks>", "Change the proxy type - default http", true);
         this.addOption(optionsBuilder, "proxy_url", "<url string>", "Change the proxy url for insights uploads", true);
         this.addOption(optionsBuilder, "proxy_authentication", "<string>", "Change the proxy authentication header string", true);
         this.addOption(optionsBuilder, "metric_sampling_interval_in_seconds", "<int>", "Change Insights metric sampling interval in seconds", false);
         this.addOption(optionsBuilder, "config_refresh_interval_in_seconds", "<int>", "Change the frequency insights configuration changes are applied to nodes", false);
         this.addOption(optionsBuilder, "data_dir_max_size_in_mb", "<int>", "Change the maximum size of the insights data directory where insights are stored if storage is enabled", false);
         this.addOption(optionsBuilder, "node_system_info_report_period", "<ISO_8601 duration string>", "Change the node system info report period(ISO-8601 duration string)", false);
         return optionsBuilder.toString();
      }

      private void addOption(StringBuilder builder, String optionName, String optionTypeHelp, String optionHelpString, boolean excludeIfServiceOptionsDisabled) {
         if(!excludeIfServiceOptionsDisabled || ((Boolean)InsightsConfig.insightsServiceOptionsEnabled.get()).booleanValue()) {
            builder.append(String.format("--%-40s%-30s%s\n", new Object[]{optionName, optionTypeHelp, optionHelpString}));
         }
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, DseToolArgumentParser parser) throws Exception {
         CommandLineParser cliParser = new DefaultParser();
         CommandLine commandLine = cliParser.parse(OPTIONS, parser.getArguments());
         if(commandLine.getOptions().length == 0) {
            System.err.println("Usage:  dsetool insights_config --option\n" + this.getOptionsHelp());
         } else {
            InsightsConfigPluginMXBean proxy = InsightsToolCommands.makeProxy(dnp);
            if(commandLine.hasOption("show_config")) {
               if(commandLine.getArgList().size() > 1) {
                  System.err.println("Cannot reconfigure Insights and show the current configuration at the same time");
               }

               System.out.println(proxy.getCurrentConfig());
            } else if(commandLine.hasOption("show_token")) {
               if(commandLine.getArgList().size() > 1) {
                  System.err.println("Cannot reconfigure Insights and show the current token at the same time");
               }

               System.out.println(proxy.getInsightsToken());
            } else {
               String modeArgValue = commandLine.getOptionValue("mode");
               if(!StringUtils.isEmpty(modeArgValue)) {
                  this.configureMode(proxy, modeArgValue);
               }

               String configRefreshInterval = commandLine.getOptionValue("config_refresh_interval_in_seconds");
               if(!StringUtils.isEmpty(configRefreshInterval)) {
                  this.send(() -> {
                     proxy.setConfigRefreshIntervalInSeconds(Integer.valueOf(Integer.parseInt(configRefreshInterval)));
                  });
                  System.out.println(String.format("Refresh interval configuration has changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String insightsUrlValue = commandLine.getOptionValue("upload_url");
               if(!StringUtils.isEmpty(insightsUrlValue)) {
                  this.send(() -> {
                     proxy.setInsightsUrl(insightsUrlValue);
                  });
                  System.out.println(String.format("Upload URL changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String insightsUploadInterval = commandLine.getOptionValue("upload_interval_in_seconds");
               if(!StringUtils.isEmpty(insightsUploadInterval)) {
                  this.send(() -> {
                     proxy.setInsightsUploadIntervalInSeconds(Integer.valueOf(Integer.parseInt(insightsUploadInterval)));
                  });
                  System.out.println(String.format("Upload interval changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String insightsMetricInterval = commandLine.getOptionValue("metric_sampling_interval_in_seconds");
               if(!StringUtils.isEmpty(insightsMetricInterval)) {
                  Integer newInterval = Integer.valueOf(Integer.parseInt(insightsMetricInterval));
                  if((long)newInterval.intValue() > InsightsConfigConstants.MAX_METRIC_UPDATE_GAP_IN_SECONDS) {
                     System.err.println("The metric sampling interval can not exceed " + InsightsConfigConstants.MAX_METRIC_UPDATE_GAP_IN_SECONDS + " seconds");
                     return;
                  }

                  this.send(() -> {
                     proxy.setMetricSamplingIntervalInSeconds(newInterval);
                  });
                  System.out.println(String.format("Metric sampling interval changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String dataDirMaxSizeInMb = commandLine.getOptionValue("data_dir_max_size_in_mb");
               if(!StringUtils.isEmpty(dataDirMaxSizeInMb)) {
                  try {
                     this.send(() -> {
                        proxy.setDataDirMaxSizeInMb(Integer.valueOf(Integer.parseInt(dataDirMaxSizeInMb)));
                     });
                  } catch (NumberFormatException var16) {
                     throw new IllegalArgumentException(String.format("%s is not a valid number", new Object[]{dataDirMaxSizeInMb}));
                  }

                  System.out.println(String.format("Config refresh interval changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String proxyType = commandLine.getOptionValue("proxy_type");
               if(!StringUtils.isEmpty(proxyType)) {
                  this.send(() -> {
                     proxy.setProxyType(proxyType);
                  });
                  System.out.println(String.format("Proxy type has changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String proxyUrl = commandLine.getOptionValue("proxy_url");
               if(!StringUtils.isEmpty(proxyUrl)) {
                  this.send(() -> {
                     proxy.setProxyUrl(proxyUrl);
                  });
                  System.out.println(String.format("Proxy URL changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

               String nodeSystemInfoReportPeriod = commandLine.getOptionValue("node_system_info_report_period");
               if(!StringUtils.isEmpty(nodeSystemInfoReportPeriod)) {
                  this.send(() -> {
                     proxy.setNodeSystemInformationReportPeriod(nodeSystemInfoReportPeriod);
                  });
                  System.out.println(String.format("Node system information report period configuration has changed. This will take at least %d seconds to take effect on all nodes", new Object[]{proxy.getConfigRefreshIntervalInSeconds()}));
               }

            }
         }
      }

      private void configureMode(InsightsConfigPluginMXBean proxy, String newMode) throws Exception {
         String currentMode = proxy.getMode();
         if(!newMode.equalsIgnoreCase(currentMode)) {
            this.send(() -> {
               proxy.setMode(newMode);
            });
            System.out.println(String.format("Insights Mode to %s.  This will take at least %d seconds to take effect on all nodes", new Object[]{newMode, proxy.getConfigRefreshIntervalInSeconds()}));
         } else {
            System.out.println(String.format("Insights Mode is already %s", new Object[]{currentMode}));
         }

      }

      private void send(InsightsToolCommands.ConfigureInsightsCommand.RunThrows r) throws Exception {
         try {
            r.run();
         } catch (RuntimeException var4) {
            Throwable root = DseUtil.getRootCause(var4);
            if(!(root instanceof InsightsRuntimeConfigAccess.AvailibilityWarning)) {
               throw var4;
            }

            System.err.println(root.getLocalizedMessage());
         }

      }

      interface RunThrows {
         void run() throws Exception;
      }
   }
}
