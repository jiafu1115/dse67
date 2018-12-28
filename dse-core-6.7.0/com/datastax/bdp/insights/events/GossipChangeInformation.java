package com.datastax.bdp.insights.events;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetAddress;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.utils.time.ApproximateTime;

public class GossipChangeInformation extends Insight {
   public static final String NAME = "dse.insights.event.gossip_change";
   private static final String MAPPING_VERSION = "dse-gossip-" + ProductVersion.getDSEVersionString();

   public GossipChangeInformation(GossipChangeInformation.Data data) {
      super(new InsightMetadata("dse.insights.event.gossip_change", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.of(MAPPING_VERSION)), data);
   }

   public GossipChangeInformation(GossipChangeInformation.GossipEventType eventType, InetAddress ipAddress, EndpointState state) {
      this(new GossipChangeInformation.Data(eventType, ipAddress, state));
   }

   public static class Data {
      @JsonProperty("event_type")
      public final GossipChangeInformation.GossipEventType eventType;
      @JsonProperty("endpoint_state")
      public final Map<ApplicationState, VersionedValue> endpointState;
      @JsonProperty("endpoint_address")
      public final String ipAddress;

      @JsonCreator
      public Data(@JsonProperty("event_type") GossipChangeInformation.GossipEventType eventType, @JsonProperty("endpoint_address") String ipAddress, @JsonProperty("endpoint_state") Map<ApplicationState, VersionedValue> endpointState) {
         this.eventType = eventType;
         this.endpointState = endpointState;
         this.ipAddress = ipAddress;
      }

      public Data(GossipChangeInformation.GossipEventType eventType, InetAddress ipAddress, EndpointState state) {
         this.eventType = eventType;
         this.ipAddress = ipAddress.getHostAddress();
         this.endpointState = new EnumMap(ApplicationState.class);
         if(state != null) {
            Iterator var4 = state.states().iterator();

            while(var4.hasNext()) {
               Entry<ApplicationState, VersionedValue> e = (Entry)var4.next();
               this.endpointState.put(e.getKey(), e.getValue());
            }
         }

      }
   }

   public static enum GossipEventType {
      JOINED,
      REMOVED,
      ALIVE,
      DEAD,
      RESTARTED;

      private GossipEventType() {
      }
   }
}
