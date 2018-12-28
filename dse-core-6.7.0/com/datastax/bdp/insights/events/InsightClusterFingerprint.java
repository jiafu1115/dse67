package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.cassandra.utils.time.ApproximateTime;

public class InsightClusterFingerprint extends Insight {
   public static final String NAME = "dse.insights.event.fingerprint";

   public InsightClusterFingerprint(List<InsightClusterFingerprint.Fingerprint> fingerprintList) {
      super(new InsightMetadata("dse.insights.event.fingerprint", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.empty()), new InsightClusterFingerprint.Data(fingerprintList));
   }

   public static class Fingerprint {
      @JsonProperty("host_id")
      public final UUID hostId;
      @JsonProperty("max_added_date_seen_by_node")
      public final Date maxAddedDateSeenByNode;
      @JsonProperty("update_date")
      public final Date updatedDate;
      @JsonProperty("token")
      public final String token;

      @JsonCreator
      public Fingerprint(@JsonProperty("host_id") UUID hostId, @JsonProperty("max_added_date_seen_by_node") Date maxAddedDateSeenByNode, @JsonProperty("update_date") Date updatedDate, @JsonProperty("token") String token) {
         this.hostId = hostId;
         this.maxAddedDateSeenByNode = maxAddedDateSeenByNode;
         this.updatedDate = updatedDate;
         this.token = token;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            InsightClusterFingerprint.Fingerprint that = (InsightClusterFingerprint.Fingerprint)o;
            return Objects.equals(this.hostId, that.hostId) && Objects.equals(this.maxAddedDateSeenByNode, that.maxAddedDateSeenByNode) && Objects.equals(this.updatedDate, that.updatedDate) && Objects.equals(this.token, that.token);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.hostId, this.maxAddedDateSeenByNode, this.updatedDate, this.token});
      }
   }

   public static class Data {
      @JsonProperty("fingerprint")
      public final List<InsightClusterFingerprint.Fingerprint> fingerprint;

      @JsonCreator
      public Data(@JsonProperty("fingerprint") List<InsightClusterFingerprint.Fingerprint> fingerprint) {
         this.fingerprint = fingerprint;
      }
   }
}
