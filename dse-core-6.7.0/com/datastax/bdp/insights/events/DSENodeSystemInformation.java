package com.datastax.bdp.insights.events;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.datastax.insights.core.json.JacksonUtil;
import com.datastax.insights.core.json.JacksonUtil.JacksonUtilException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.reactivex.functions.Function;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.time.ApproximateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DSENodeSystemInformation extends Insight {
   private static final Logger logger = LoggerFactory.getLogger(DSENodeSystemInformation.class);
   public static final String NAME = "dse.insights.event.node_system_information";
   private static final String MAPPING_VERSION = "dse-node-info-" + ProductVersion.getDSEVersionString();

   @JsonCreator
   public DSENodeSystemInformation(@JsonProperty("metadata") InsightMetadata metadata, @JsonProperty("data") DSENodeSystemInformation.Data data) {
      super(metadata, data);
   }

   public DSENodeSystemInformation(DSENodeSystemInformation.Data data) {
      super(new InsightMetadata("dse.insights.event.node_system_information", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.of(MAPPING_VERSION)), data);
   }

   public DSENodeSystemInformation() {
      this(new DSENodeSystemInformation.Data());
   }

   @JsonIgnore
   public DSENodeSystemInformation.Data getData() {
      return (DSENodeSystemInformation.Data)this.insightData;
   }

   public static class Data {
      @JsonProperty("local_info")
      @JsonRawValue
      public final String localInfo;
      @JsonProperty("peers_info")
      @JsonRawValue
      public final String peersInfo;

      @JsonCreator
      @VisibleForTesting
      public Data(@JsonProperty("local_info") Object localInfo, @JsonProperty("peers_info") Object peersInfo) {
         try {
            this.localInfo = JacksonUtil.writeValueAsString(localInfo);
         } catch (JacksonUtilException var5) {
            throw new IllegalArgumentException("Error creating localInfo json", var5);
         }

         try {
            this.peersInfo = JacksonUtil.writeValueAsString(peersInfo);
         } catch (JacksonUtilException var4) {
            throw new IllegalArgumentException("Error creating peersInfo json", var4);
         }
      }

      private Data() {
         this.localInfo = ((UntypedResultSet)SystemKeyspace.readLocalTableJSON().join()).one().getString("[json]");
         this.peersInfo = "[" + Joiner.on(",").join((Iterable)((UntypedResultSet)SystemKeyspace.readPeersTableJSON().join()).rows().map((r) -> {
            return r.getString("[json]");
         }).toList().blockingSingle()) + "]";
      }

      @JsonIgnore
      @VisibleForTesting
      public List<Map<String, Object>> getPeersInfoList() {
         try {
            return (List)JacksonUtil.readValue(this.peersInfo, List.class);
         } catch (JacksonUtilException var2) {
            throw new RuntimeException(var2);
         }
      }

      @JsonIgnore
      @VisibleForTesting
      public Map<String, Object> getLocalInfoMap() {
         try {
            return JacksonUtil.convertJsonToMap(this.localInfo);
         } catch (JacksonUtilException var2) {
            throw new RuntimeException(var2);
         }
      }
   }
}
