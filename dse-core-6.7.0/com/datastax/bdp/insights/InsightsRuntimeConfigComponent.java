package com.datastax.bdp.insights;

import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import java.util.Optional;

public interface InsightsRuntimeConfigComponent {
   void start();

   void stop();

   boolean isStarted();

   default boolean shouldRestart(InsightsRuntimeConfig oldConfig, InsightsRuntimeConfig newConfig) {
      return false;
   }

   Optional<String> getNameForFiltering();
}
