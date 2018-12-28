package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Optional;
import org.apache.cassandra.utils.time.ApproximateTime;

public class SchemaChangeInformation extends Insight {
   public static final String NAME = "dse.insights.event.schema_change";

   public SchemaChangeInformation(SchemaChangeInformation.ChangeType changeType, String keyspace, String name) {
      super(new InsightMetadata("dse.insights.event.schema_change", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.empty()), new SchemaChangeInformation.Data(changeType, keyspace, name));
   }

   public SchemaChangeInformation(SchemaChangeInformation.ChangeType changeType, String keyspace) {
      this(changeType, keyspace, (String)null);
   }

   @JsonInclude(Include.NON_NULL)
   public static class Data {
      @JsonProperty("change_type")
      public final SchemaChangeInformation.ChangeType changeType;
      @JsonProperty("keyspace")
      public final String keyspace;
      @JsonProperty("name")
      public final String name;

      public Data(SchemaChangeInformation.ChangeType changeType, String keyspace, String name) {
         this.changeType = changeType;
         this.keyspace = keyspace;
         this.name = name;
      }
   }

   public static enum ChangeType {
      CREATE_KEYSPACE,
      ALTER_KEYSPACE,
      DROP_KEYSPACE,
      CREATE_TABLE,
      ALTER_TABLE,
      DROP_TABLE,
      CREATE_VIEW,
      ALTER_VIEW,
      DROP_VIEW,
      CREATE_TYPE,
      ALTER_TYPE,
      DROP_TYPE,
      CREATE_FUNCTION,
      ALTER_FUNCTION,
      DROP_FUNCTION,
      CREATE_AGGREGATE,
      ALTER_AGGREGATE,
      DROP_AGGREGATE,
      CREATE_VIRTUAL_KEYSPACE,
      CREATE_VIRTUAL_TABLE;

      private ChangeType() {
      }
   }
}
