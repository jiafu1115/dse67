package com.datastax.bdp.insights.rpc;

import com.datastax.bdp.util.rpc.Rpc;
import com.datastax.bdp.util.rpc.RpcParam;
import com.datastax.insights.client.InsightsClient;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.json.JacksonUtil;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.auth.permission.CorePermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class InsightsRpc {
   private static final Logger logger = LoggerFactory.getLogger(InsightsRpc.class);
   public static final String FILTER_NAME = "dse.insights.event.driver_rpc";
   public static final String NAME = "InsightsRpc";
   private static final AtomicBoolean allowed = new AtomicBoolean(true);
   private final InsightsClient insightsClient;
   public static final String REPORT_INSIGHT = "reportInsight";

   @Inject
   public InsightsRpc(InsightsClient insightsClient) {
      this.insightsClient = insightsClient;
   }

   @Rpc(
      name = "reportInsight",
      permission = CorePermission.EXECUTE
   )
   public void reportInsight(@RpcParam(name = "json") String json) throws Exception {
      Insight insight;
      String errorMessage;
      try {
         insight = (Insight)JacksonUtil.getObjectMapper().readValue(json, Insight.class);
      } catch (IOException var6) {
         errorMessage = String.format("Error converting JSON to a valid Insight.  JSON received: %s, Error: ", new Object[]{json});
         logger.error(errorMessage, var6);
         throw new RuntimeException(errorMessage, var6);
      }

      try {
         if(!allowed.get()) {
            logger.trace("Filtering driver rpc insight call {}", json);
         } else {
            this.insightsClient.report(insight);
         }
      } catch (Exception var5) {
         errorMessage = String.format("Error publishing Insight: %s, Error: ", new Object[]{insight});
         logger.error(errorMessage, var5);
         throw new RuntimeException(errorMessage, var5);
      }
   }

   public void setFiltered() {
      allowed.compareAndSet(true, false);
   }

   public void setAllowed() {
      allowed.compareAndSet(false, true);
   }
}
