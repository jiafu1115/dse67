package com.datastax.bdp.tools;

import java.util.concurrent.TimeUnit;

public class InsightsConfigConstants {
   public static final String CONFIG_COMMAND_DESCRIPTION = "Insights Configuration";
   public static final String CONFIG_COMMAND_NAME = "insights_config";
   public static final String MODE_HELP = "Sets the Insights mode";
   public static final String MODE_OPTION = "mode";
   public static final String SHOW_CONFIG_OPTION = "show_config";
   public static final String SHOW_CONFIG_HELP = "Show current Insights configuration";
   public static final String SHOW_TOKEN_OPTION = "show_token";
   public static final String SHOW_TOKEN_HELP = "Show current Insights service provided token for this node";
   public static final String UPLOAD_URL_HELP = "Change Insights Service URL to upload data to";
   public static final String UPLOAD_URL_OPTION = "upload_url";
   public static final String UPLOAD_INTERVAL_IN_SECONDS_HELP = "Change upload interval in seconds to the Insights service";
   public static final String UPLOAD_INTERVAL_IN_SECONDS_OPTION = "upload_interval_in_seconds";
   public static final String METRIC_SAMPLING_INTERVAL_IN_SECONDS_HELP = "Change Insights metric sampling interval in seconds";
   public static final String METRIC_SAMPLING_INTERVAL_IN_SECONDS_OPTION = "metric_sampling_interval_in_seconds";
   public static final String CONFIG_REFRESH_INTERVAL_HELP = "Change the frequency insights configuration changes are applied to nodes";
   public static final String CONFIG_REFRESH_INTERVAL_OPTION = "config_refresh_interval_in_seconds";
   public static final String DATA_DIR_MAX_SIZE_IN_MB_HELP = "Change the maximum size of the insights data directory where insights are stored if storage is enabled";
   public static final String DATA_DIR_MAX_SIZE_IN_MB_OPTION = "data_dir_max_size_in_mb";
   public static final String PROXY_TYPE_HELP = "Change the proxy type - default http";
   public static final String PROXY_TYPE_OPTION = "proxy_type";
   public static final String PROXY_URL_HELP = "Change the proxy url for insights uploads";
   public static final String PROXY_URL_OPTION = "proxy_url";
   public static final String PROXY_AUTHENTICATION_HELP = "Change the proxy authentication header string";
   public static final String PROXY_AUTHENTICATION_OPTION = "proxy_authentication";
   public static final String NODE_SYSTEM_INFO_HELP = "Change the node system info report period(ISO-8601 duration string)";
   public static final String NODE_SYSTEM_INFO_REPORT_PERIOD_OPTION = "node_system_info_report_period";
   public static final String FILTER_COMMAND_NAME = "insights_filters";
   public static final String FILTER_SHOW_HELP = "Display the existing filtering rules";
   public static final String FILTER_SHOW_OPTION = "show_filters";
   public static final String FILTER_CLEARALL_HELP = "Remove all the existing filtering rules (allows all)";
   public static final String FILTER_CLEARALL_OPTION = "remove_all_filters";
   public static final String FILTER_SCOPE_GLOBAL_HELP = "Set the filtering rule scope to 'global': meaning, rule affects insights and collectd reporting";
   public static final String FILTER_SCOPE_GLOBAL_OPTION = "global";
   public static final String FILTER_SCOPE_INSIGHTS_HELP = "Set the filtering rule scope to 'insights_only': meaning, rule affects insights reporting only and *not* collectd";
   public static final String FILTER_SCOPE_INSIGHTS_OPTION = "insights_only";
   public static final String FILTER_ADD_HELP = "Add a filtering rule to the list";
   public static final String FILTER_ADD_OPTION = "add";
   public static final String FILTER_REMOVE_HELP = "Remove a filtering rule from the list";
   public static final String FILTER_REMOVE_OPTION = "remove";
   public static final String FILTER_ALLOW_HELP = "Set the filtering rule type to 'allow': meaning, any name that matches this rule will be allowed through";
   public static final String FILTER_ALLOW_OPTION = "allow";
   public static final String FILTER_DENY_HELP = "Set the filtering rule type to 'deny': meaning, any name that matches this rule will be blocked";
   public static final String FILTER_DENY_OPTION = "deny";
   public static final long MAX_METRIC_UPDATE_GAP_IN_SECONDS;

   public InsightsConfigConstants() {
   }

   static {
      MAX_METRIC_UPDATE_GAP_IN_SECONDS = TimeUnit.MINUTES.toSeconds(5L);
   }
}
