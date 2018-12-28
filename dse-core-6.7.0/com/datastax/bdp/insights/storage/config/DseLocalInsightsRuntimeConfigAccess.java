package com.datastax.bdp.insights.storage.config;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.config.InsightsConfig;
import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.bdp.db.util.ProductVersion.Version;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.datastax.insights.core.json.JacksonUtil;
import com.datastax.insights.core.json.JacksonUtil.JacksonUtilException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DseLocalInsightsRuntimeConfigAccess implements InsightsRuntimeConfigAccess {
   private static final Logger logger = LoggerFactory.getLogger(DseLocalInsightsRuntimeConfigAccess.class);
   private static final ObjectMapper MAPPER = new ObjectMapper();
   private static final String INSERT_CONFIG_CQL = "INSERT INTO %s.%s JSON \n'\n{\n  \"key\" : 1, \n  \"config\" : {\n     \"mode\" : \"%s\",\n     \"upload_url\" : \"%s\",\n     \"upload_interval_in_seconds\" : %d,\n     \"metric_sampling_interval_in_seconds\" : %d,\n     \"data_dir_max_size_in_mb\" : %d,\n     \"proxy_type\" : \"%s\",\n     \"proxy_url\" : \"%s\",\n     \"proxy_authentication\" : \"%s\",\n     \"node_system_info_report_period\" : \"%s\",\n     \"config_refresh_interval_in_seconds\" : %d,\n     \"filtering_rules\" : %s\n  } \n}\n' USING TIMESTAMP 0";

   @Inject
   public DseLocalInsightsRuntimeConfigAccess() {
   }

   public void updateConfig(InsightsRuntimeConfig insightsRuntimeConfig, String keyspace) {
      try {
         this.validateKeyspace(keyspace);
         String insertConfigDefaults = String.format("INSERT INTO %s.%s JSON \n'\n{\n  \"key\" : 1, \n  \"config\" : {\n     \"mode\" : \"%s\",\n     \"upload_url\" : \"%s\",\n     \"upload_interval_in_seconds\" : %d,\n     \"metric_sampling_interval_in_seconds\" : %d,\n     \"data_dir_max_size_in_mb\" : %d,\n     \"proxy_type\" : \"%s\",\n     \"proxy_url\" : \"%s\",\n     \"proxy_authentication\" : \"%s\",\n     \"node_system_info_report_period\" : \"%s\",\n     \"config_refresh_interval_in_seconds\" : %d,\n     \"filtering_rules\" : %s\n  } \n}\n' USING TIMESTAMP 0", new Object[]{keyspace, "insights_config", insightsRuntimeConfig.config.mode, insightsRuntimeConfig.config.upload_url, insightsRuntimeConfig.config.upload_interval_in_seconds, insightsRuntimeConfig.config.metric_sampling_interval_in_seconds, insightsRuntimeConfig.config.data_dir_max_size_in_mb, insightsRuntimeConfig.config.proxy_type, insightsRuntimeConfig.config.proxy_url, insightsRuntimeConfig.config.proxy_authentication, insightsRuntimeConfig.config.node_system_info_report_period, insightsRuntimeConfig.config.config_refresh_interval, JacksonUtil.writeValueAsString(insightsRuntimeConfig.config.filtering_rules)});
         Set<InetAddress> unreachables = Gossiper.instance.getUnreachableTokenOwners();
         boolean someOffline = !unreachables.isEmpty();
         QueryProcessorUtil.execute(insertConfigDefaults.replaceAll("USING TIMESTAMP 0", ""), ConsistencyLevel.LOCAL_ONE, new Object[0]);
         if("dse_insights".equalsIgnoreCase(keyspace) && someOffline) {
            throw new RuntimeException(new InsightsRuntimeConfigAccess.AvailibilityWarning("WARNING: Some nodes unreachable, this will delay the config change on these nodes: " + Arrays.toString(unreachables.toArray())));
         }
      } catch (JacksonUtilException var6) {
         throw new RuntimeException(var6);
      }
   }

   private void validateKeyspace(String keyspace) {
      if(!"dse_insights".equalsIgnoreCase(keyspace) && !"dse_insights_local".equalsIgnoreCase(keyspace)) {
         throw new IllegalArgumentException("Invalid insights config keyspace specified");
      }
   }

   public void createConfigDefaultsIfNecessary(String keyspace) {
      this.validateKeyspace(keyspace);
      String defaultFilteringRules = "[]";

      try {
         defaultFilteringRules = JacksonUtil.writeValueAsString(InsightsConfig.DEFAULT_CONFIG_FILTERING_RULES);
      } catch (JacksonUtilException var4) {
         logger.error("Cannot create default metric filtering rules due to error: ", var4);
      }

      String insertConfigDefaults = String.format("INSERT INTO %s.%s JSON \n'\n{\n  \"key\" : 1, \n  \"config\" : {\n     \"mode\" : \"%s\",\n     \"upload_url\" : \"%s\",\n     \"upload_interval_in_seconds\" : %d,\n     \"metric_sampling_interval_in_seconds\" : %d,\n     \"data_dir_max_size_in_mb\" : %d,\n     \"proxy_type\" : \"%s\",\n     \"proxy_url\" : \"%s\",\n     \"proxy_authentication\" : \"%s\",\n     \"node_system_info_report_period\" : \"%s\",\n     \"config_refresh_interval_in_seconds\" : %d,\n     \"filtering_rules\" : %s\n  } \n}\n' USING TIMESTAMP 0", new Object[]{keyspace, "insights_config", InsightsConfig.DEFAULT_MODE_SETTING, InsightsConfig.DEFAULT_INSIGHTS_URL, Integer.valueOf(300), Integer.valueOf(30), Integer.valueOf(1024), "http", "", "", "PT1H", Integer.valueOf(30), defaultFilteringRules});
      QueryProcessorUtil.execute(insertConfigDefaults, ConsistencyLevel.LOCAL_ONE, new Object[0]);
   }

   public InsightsRuntimeConfig selectConfigValue(String keyspace) {
      Version minVersion = Gossiper.instance.clusterVersionBarrier.currentClusterVersionInfo().minDse;
      if("dse_insights".equalsIgnoreCase(keyspace) && minVersion.asReleaseVersion().compareTo(ProductVersion.DSE_VERSION_67) < 0) {
         return null;
      } else {
         String SELECT_CONFIG_CQL = "SELECT JSON %s from %s.%s where key = 1";
         String selectValue = String.format("SELECT JSON %s from %s.%s where key = 1", new Object[]{"config", keyspace, "insights_config"});
         SelectStatement selectStatement = (SelectStatement)StatementUtils.prepareStatementBlocking(selectValue, QueryState.forInternalCalls(), String.format("Error preparing \"%s\" statement", new Object[]{selectValue}));
         UntypedResultSet resultSet = QueryProcessorUtil.processPreparedSelect(selectStatement, ConsistencyLevel.LOCAL_ONE);
         if(resultSet.isEmpty()) {
            return null;
         } else {
            try {
               String json = resultSet.one().getString("[json]");
               return (InsightsRuntimeConfig)MAPPER.readValue(json, InsightsRuntimeConfig.class);
            } catch (IOException var8) {
               throw new RuntimeException("Error parsing insights config json", var8);
            }
         }
      }
   }
}
