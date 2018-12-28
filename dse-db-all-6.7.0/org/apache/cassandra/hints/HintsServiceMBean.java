package org.apache.cassandra.hints;

import java.util.Map;

public interface HintsServiceMBean {
   void pauseDispatch();

   void resumeDispatch();

   void deleteAllHints();

   Map<String, Map<String, String>> listEndpointsPendingHints();

   void deleteAllHintsForEndpoint(String var1);
}
