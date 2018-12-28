package com.datastax.bdp.insights.events;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.datastax.insights.core.json.JacksonUtil;
import com.datastax.insights.core.json.JacksonUtil.JacksonUtilException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.reactivex.functions.Function;
import java.util.Optional;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.time.ApproximateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DSESchemaInformation extends Insight {
   private static final String NAME = "dse.insights.events.schema_information";
   private static final String MAPPING_VERSION = "dse-schema-" + ProductVersion.getDSEVersionString();
   private static final Logger logger = LoggerFactory.getLogger(DSESchemaInformation.class);

   @JsonCreator
   public DSESchemaInformation(@JsonProperty("metadata") InsightMetadata metadata, @JsonProperty("data") DSESchemaInformation.Data data) {
      super(metadata, data);
   }

   public DSESchemaInformation(DSESchemaInformation.Data data) {
      super(new InsightMetadata("dse.insights.events.schema_information", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.of(MAPPING_VERSION)), data);
   }

   public DSESchemaInformation() {
      this(new DSESchemaInformation.Data());
   }

   public DSESchemaInformation.Data getData() {
      return (DSESchemaInformation.Data)this.insightData;
   }

   public static class Data {
      @JsonRawValue
      @JsonProperty("keyspaces")
      public final String keyspaces;
      @JsonRawValue
      @JsonProperty("tables")
      public final String tables;
      @JsonRawValue
      @JsonProperty("columns")
      public final String columns;
      @JsonRawValue
      @JsonProperty("dropped_columns")
      public final String dropped_columns;
      @JsonRawValue
      @JsonProperty("triggers")
      public final String triggers;
      @JsonRawValue
      @JsonProperty("views")
      public final String views;
      @JsonRawValue
      @JsonProperty("types")
      public final String types;
      @JsonRawValue
      @JsonProperty("functions")
      public final String functions;
      @JsonRawValue
      @JsonProperty("aggregates")
      public final String aggregates;
      @JsonRawValue
      @JsonProperty("indexes")
      public final String indexes;

      @JsonCreator
      @VisibleForTesting
      public Data(@JsonProperty("keyspaces") Object keyspaces, @JsonProperty("tables") Object tables, @JsonProperty("columns") Object columns, @JsonProperty("dropped_columns") Object dropped_columns, @JsonProperty("triggers") Object triggers, @JsonProperty("views") Object views, @JsonProperty("types") Object types, @JsonProperty("functions") Object functions, @JsonProperty("aggregates") Object aggregates, @JsonProperty("indexes") Object indexes) {
         try {
            this.keyspaces = JacksonUtil.writeValueAsString(keyspaces);
            this.tables = JacksonUtil.writeValueAsString(tables);
            this.columns = JacksonUtil.writeValueAsString(columns);
            this.dropped_columns = JacksonUtil.writeValueAsString(dropped_columns);
            this.triggers = JacksonUtil.writeValueAsString(triggers);
            this.views = JacksonUtil.writeValueAsString(views);
            this.types = JacksonUtil.writeValueAsString(types);
            this.functions = JacksonUtil.writeValueAsString(functions);
            this.aggregates = JacksonUtil.writeValueAsString(aggregates);
            this.indexes = JacksonUtil.writeValueAsString(indexes);
         } catch (JacksonUtilException var12) {
            throw new IllegalArgumentException("Error creating schema info", var12);
         }
      }

      private Data() {
         this.keyspaces = this.tableToJson("keyspaces");
         this.tables = this.tableToJson("tables");
         this.columns = this.tableToJson("columns");
         this.dropped_columns = this.tableToJson("dropped_columns");
         this.triggers = this.tableToJson("triggers");
         this.views = this.tableToJson("views");
         this.types = this.tableToJson("types");
         this.functions = this.tableToJson("functions");
         this.aggregates = this.tableToJson("aggregates");
         this.indexes = this.tableToJson("indexes");
      }

      private String tableToJson(String table) {
         String cql = String.format("SELECT JSON * from %s.%s", new Object[]{"system_schema", table});
         String json = "[";

         try {
            UntypedResultSet rs = QueryProcessor.executeInternal(cql, new Object[0]);
            json = json + Joiner.on(",").join((Iterable)rs.rows().map((r) -> {
               return r.getString("[json]");
            }).toList().blockingSingle());
         } catch (Exception var5) {
            DSESchemaInformation.logger.warn("Error fetching schema information on {}.{}", new Object[]{"system_schema", table, var5});
         }

         json = json + "]";
         return json;
      }
   }
}
