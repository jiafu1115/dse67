package com.datastax.bdp.snitch;

import com.datastax.bdp.gms.DseState;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;

public class EndpointStateTrackerBase {
   private final Gossiper gossiper;

   public EndpointStateTrackerBase() {
      this.gossiper = Gossiper.instance;
   }

   public String getDatacenter(InetAddress endpoint) {
      return SystemKeyspace.getDatacenter(endpoint);
   }

   public Set<Workload> getWorkloads(InetAddress endpoint) {
      return SystemKeyspace.getWorkloadsBestEffort(endpoint);
   }

   public Set<Workload> getWorkloadsIfPresent(InetAddress endpoint) {
      return SystemKeyspace.getWorkloadsIfPresent(endpoint);
   }

   public Map<String, Set<Workload>> getDatacenterWorkloads() {
      return SystemKeyspace.getDatacenterWorkloads();
   }

   public String getLegacyWorkload(InetAddress endpoint) {
      return Workload.legacyWorkloadName(this.getWorkloads(endpoint));
   }

   public boolean getIsGraphServer(InetAddress endpoint) {
      Boolean b = SystemKeyspace.isGraphNode(endpoint);
      return b != null && b.booleanValue();
   }

   public String getServerId(InetAddress endpoint) {
      return SystemKeyspace.getServerId(endpoint);
   }

   public Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(InetAddress endpoint) {
      return StorageService.instance.getCoreIndexingStatus(endpoint);
   }

   public Map<String, Long> getAllKnownDatacenters() {
      return SystemKeyspace.getAllKnownDatacenters();
   }

   public Double getNodeHealth(InetAddress endpoint) {
      return StorageService.instance.getNodeHealth(endpoint);
   }

   public boolean getActiveStatus(InetAddress endpoint) {
      return StorageService.instance.getActiveStatus(endpoint);
   }

   public boolean isActive(InetAddress endpoint) {
      return this.getActiveStatus(endpoint) && Gossiper.instance.isAlive(endpoint);
   }

   public boolean getBlacklistedStatus(InetAddress endpoint) {
      return StorageService.instance.getBlacklistedStatus(endpoint);
   }
}
