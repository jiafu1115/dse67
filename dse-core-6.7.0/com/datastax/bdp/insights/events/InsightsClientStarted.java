package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import java.time.Instant;

public class InsightsClientStarted extends Insight {
   public static final String NAME = "dse.insights.event.insights_client_started";

   public InsightsClientStarted() {
      super(new InsightMetadata("dse.insights.event.insights_client_started", Long.valueOf(Instant.now().toEpochMilli())), (Object)null);
   }
}
