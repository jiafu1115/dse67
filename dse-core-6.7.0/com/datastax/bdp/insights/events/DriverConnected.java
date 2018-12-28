package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class DriverConnected extends Insight {
   public static final String NAME = "dse.insights.event.driver_connected";

   @JsonCreator
   public DriverConnected(@JsonProperty("driverVersion") String driverVersion) {
      super(new InsightMetadata("dse.insights.event.driver_connected", Long.valueOf(Instant.now().toEpochMilli())), new DriverConnected.Data(driverVersion));
   }

   private static final class Data {
      @JsonProperty("driverVersion")
      public final String driverVersion;

      @JsonCreator
      public Data(@JsonProperty("driverVersion") String driverVersion) {
         this.driverVersion = driverVersion;
      }
   }
}
