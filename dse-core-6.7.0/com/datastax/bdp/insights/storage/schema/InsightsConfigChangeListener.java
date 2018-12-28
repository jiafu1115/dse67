package com.datastax.bdp.insights.storage.schema;

import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;

public interface InsightsConfigChangeListener {
   void onConfigChanged(InsightsRuntimeConfig var1, InsightsRuntimeConfig var2);
}
