package com.datastax.bdp.insights.storage.config;

public interface InsightsRuntimeConfigAccess {
   void updateConfig(InsightsRuntimeConfig var1, String var2);

   void createConfigDefaultsIfNecessary(String var1);

   InsightsRuntimeConfig selectConfigValue(String var1);

   public static class AvailibilityWarning extends Exception {
      AvailibilityWarning(String msg) {
         super(msg);
      }
   }
}
