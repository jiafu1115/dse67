package org.apache.cassandra.config;

import javax.net.ssl.SSLSocketFactory;

public abstract class EncryptionOptions {
   public String keystore = "conf/.keystore";
   public String keystore_password = "cassandra";
   public String truststore = "conf/.truststore";
   public String truststore_password = "cassandra";
   public String[] cipher_suites = ((SSLSocketFactory)SSLSocketFactory.getDefault()).getDefaultCipherSuites();
   public String protocol = "TLS";
   public String algorithm = "SunX509";
   /** @deprecated */
   @Deprecated
   public String store_type;
   public String keystore_type;
   public String truststore_type;
   public boolean require_client_auth = false;
   public boolean require_endpoint_verification = false;

   public EncryptionOptions() {
   }

   public String getKeystoreType() {
      return this.store_type != null?this.store_type:this.keystore_type;
   }

   public String getTruststoreType() {
      return this.store_type != null?this.store_type:this.truststore_type;
   }

   public static class ServerEncryptionOptions extends EncryptionOptions {
      public EncryptionOptions.ServerEncryptionOptions.InternodeEncryption internode_encryption;

      public ServerEncryptionOptions() {
         this.internode_encryption = EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none;
      }

      public static enum InternodeEncryption {
         all,
         none,
         dc,
         rack;

         private InternodeEncryption() {
         }
      }
   }

   public static class ClientEncryptionOptions extends EncryptionOptions {
      public boolean enabled = false;
      public boolean optional = false;

      public ClientEncryptionOptions() {
      }
   }
}
