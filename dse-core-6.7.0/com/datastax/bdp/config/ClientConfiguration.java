package com.datastax.bdp.config;

import com.datastax.bdp.transport.common.ServicePrincipal;
import java.net.InetAddress;

public interface ClientConfiguration {
   String DEFAULT_KEYSTORE_TYPE = "JKS";
   String DEFAULT_TRUSTSTORE_TYPE = "JKS";
   String PKCS11_STORE_TYPE = "PKCS11";

   ServicePrincipal getDseServicePrincipal();

   ServicePrincipal getHttpServicePrincipal();

   String getSaslProtocolName();

   boolean isSslEnabled();

   boolean isSslOptional();

   String getSslKeystorePath();

   String getSslKeystorePassword();

   String getSslKeystoreType();

   String getSslTruststorePath();

   String getSslTruststorePassword();

   String getSslTruststoreType();

   String getSslProtocol();

   String getSslAlgorithm();

   String[] getCipherSuites();

   boolean isKerberosEnabled();

   boolean isKerberosDefaultScheme();

   String getSaslQop();

   int getNativePort();

   InetAddress getCassandraHost();

   String getPartitionerClassName();

   int getDseFsPort();

   String getCdcRawDirectory();

   String getAdvancedReplicationDirectory();

   default InetAddress[] getCassandraHosts() {
      InetAddress cassandraHost = this.getCassandraHost();
      return cassandraHost != null?new InetAddress[]{this.getCassandraHost()}:new InetAddress[0];
   }
}
