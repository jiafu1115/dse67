package com.datastax.bdp.insights;

public enum InsightsMode {
   DISABLED,
   ENABLED_NO_STORAGE,
   ENABLED_WITH_LOCAL_STORAGE,
   ENABLED_WITH_UPLOAD,
   ENABLED_WITH_STREAMING;

   private InsightsMode() {
   }
}
