package com.datastax.bdp.insights.storage.schema;

import com.datastax.bdp.util.QueryProcessorUtil;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.locator.EverywhereStrategy;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.schema.TableMetadata.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public final class InsightsSchemaManager {
   private static final Logger logger = LoggerFactory.getLogger(InsightsSchemaManager.class);
   public static final String CONFIG_KEY_COLUMN = "key";
   public static final String CONFIG_VALUE_COLUMN = "config";
   private final UserType ruleLocalType = this.ruleType("dse_insights_local");
   private final UserType ruleGlobalType = this.ruleType("dse_insights");
   private final UserType configLocalType;
   private final UserType configGlobalType;
   private final NodeSyncParams globalNodeSyncParams;
   private final TableMetadata localConfigTableMetadata;
   private final TableMetadata globalConfigTableMetadata;
   private final TableMetadata globalTokensTableMetadata;

   @Inject
   public InsightsSchemaManager() {
      this.configLocalType = this.configType("dse_insights_local", this.ruleLocalType);
      this.configGlobalType = this.configType("dse_insights", this.ruleGlobalType);
      this.globalNodeSyncParams = new NodeSyncParams(Boolean.valueOf(true), Integer.valueOf(1200), Collections.EMPTY_MAP);
      this.localConfigTableMetadata = this.table("dse_insights_local", "insights_config", "CREATE TABLE IF NOT EXISTS %s (key int PRIMARY KEY, " + String.format("%s %s )", new Object[]{"config", "insights_config_type"}), Lists.newArrayList(new UserType[]{this.ruleLocalType, this.configLocalType})).comment("DSE Insights Config").build();
      this.globalConfigTableMetadata = this.table("dse_insights", "insights_config", "CREATE TABLE IF NOT EXISTS %s (key int PRIMARY KEY, " + String.format("%s %s )", new Object[]{"config", "insights_config_type"}), Lists.newArrayList(new UserType[]{this.ruleGlobalType, this.configGlobalType})).comment("DSE Insights Config").nodesync(this.globalNodeSyncParams).build();
      this.globalTokensTableMetadata = this.table("dse_insights", "tokens", "CREATE TABLE IF NOT EXISTS %s (node UUID PRIMARY KEY, bearer_token ascii,max_added_date_seen_by_node timestamp,last_updated timestamp)", Collections.emptyList()).comment("DSE Insights Tokens").nodesync(this.globalNodeSyncParams).build();
   }

   public void setupSchema() {
      try {
         SchemaTool.maybeCreateOrUpdateKeyspace(KeyspaceMetadata.create("dse_insights_local", KeyspaceParams.local(), Tables.none(), Views.none(), Types.of(new UserType[]{this.configLocalType, this.ruleLocalType}), Functions.none()), 0L);
         SchemaTool.maybeCreateTable("dse_insights_local", "insights_config", this.localConfigTableMetadata);
         SchemaTool.maybeCreateOrUpdateKeyspace(KeyspaceMetadata.create("dse_insights", KeyspaceParams.create(true, ImmutableMap.of("class", EverywhereStrategy.class.getCanonicalName())), Tables.of(new TableMetadata[]{this.globalConfigTableMetadata, this.globalTokensTableMetadata}), Views.none(), Types.of(new UserType[]{this.configGlobalType, this.ruleGlobalType}), Functions.none()), 0L);
      } catch (Exception var2) {
         logger.error("Error setting up insights schema", var2);
      }

   }

   private UserType configType(String keyspace, UserType ruleType) {
      return CreateTypeStatement.parse(String.format("CREATE TYPE %s (\n    mode text,\n    upload_url text,\n    upload_interval_in_seconds int,\n    metric_sampling_interval_in_seconds int,\n    data_dir_max_size_in_mb int,\n    proxy_type text,\n    proxy_url text,\n    proxy_authentication text,\n    node_system_info_report_period text,\n    config_refresh_interval_in_seconds int,\n    filtering_rules frozen<set<%s>>\n);", new Object[]{"insights_config_type", "insights_filters_rule_type"}), keyspace, Types.of(new UserType[]{ruleType}));
   }

   private UserType ruleType(String keyspace) {
      return CreateTypeStatement.parse(String.format("CREATE TYPE %s (\n    policy text,\n    pattern text,\n    scope text\n);", new Object[]{"insights_filters_rule_type"}), keyspace);
   }

   private Builder table(String keyspace, String table, String schema, Collection<UserType> types) {
      return CreateTableStatement.parse(String.format(schema, new Object[]{QueryProcessorUtil.getFullTableName(keyspace, table)}), keyspace, types).id(SchemaTool.tableIdForDseSystemTable(keyspace, table)).gcGraceSeconds(0).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L));
   }
}
