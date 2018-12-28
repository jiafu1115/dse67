package com.datastax.bdp.insights.storage.schema;

public class InsightsSchemaConstants {
   public static final String GLOBAL_INSIGHTS_KEYSPACE = "dse_insights";
   public static final String LOCAL_INSIGHTS_KEYSPACE = "dse_insights_local";
   public static final String INSIGHTS_CONFIG_TABLE = "insights_config";
   public static final String INSIGHTS_CONFIG = "insights_config_type";
   public static final String INSIGHTS_FILTERING_RULES_TYPE = "insights_filters_rule_type";
   public static final String INSIGHTS_TOKENS = "tokens";
   public static final String FILTERING_RULES_UDT_FIELD = "filtering_rules";

   public InsightsSchemaConstants() {
   }
}
