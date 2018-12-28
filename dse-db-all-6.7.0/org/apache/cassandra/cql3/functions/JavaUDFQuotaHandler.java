package org.apache.cassandra.cql3.functions;

import java.util.Set;

final class JavaUDFQuotaHandler {
   private JavaUDFQuotaHandler() {
   }

   static void beforeStart(UDFQuotaState quotaState) {
      ThreadAwareSecurityManager.enterSecureSection(quotaState, (Set)null).onStart();
   }

   static boolean udfExecCall() {
      return ThreadAwareSecurityManager.secureSection().failCheckLazy();
   }

   static void afterExec(UDFExecResult result) {
      ThreadAwareSecurityManager.leaveSecureSection().afterExec(result);
   }
}
