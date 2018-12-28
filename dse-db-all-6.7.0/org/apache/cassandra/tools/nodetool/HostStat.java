package org.apache.cassandra.tools.nodetool;

import java.net.InetAddress;

public class HostStat {
   public final InetAddress endpoint;
   public final boolean resolveIp;
   public final Float owns;
   public final String token;

   public HostStat(String token, InetAddress endpoint, boolean resolveIp, Float owns) {
      this.token = token;
      this.endpoint = endpoint;
      this.resolveIp = resolveIp;
      this.owns = owns;
   }

   public String ipOrDns() {
      return this.resolveIp?this.endpoint.getHostName():this.endpoint.getHostAddress();
   }
}
