package com.datastax.bdp.snitch;

import com.datastax.bdp.gms.DseState;
import com.datastax.bdp.gms.DseState.CoreIndexingStatus;
import com.datastax.bdp.ioc.DseInjector;
import com.datastax.bdp.leasemanager.LeasePlugin;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.rpc.Rpc;
import com.datastax.bdp.util.rpc.RpcParam;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointStateTracker extends EndpointStateTrackerBase implements EndpointStateTrackerMXBean {
   private static final Logger logger = LoggerFactory.getLogger(EndpointStateTracker.class);
   public static final EndpointStateTracker instance = new EndpointStateTracker();

   public EndpointStateTracker() {
   }

   public boolean getIsGraphServer(String endpoint) {
      try {
         return this.getIsGraphServer(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public String getDatacenter(String endpoint) {
      try {
         return this.getDatacenter(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   @Rpc(
      name = "getWorkload",
      permission = CorePermission.SELECT
   )
   public String getWorkloads(@RpcParam(name = "endpoint") String endpoint) {
      try {
         return Workload.workloadNames(this.getWorkloads(InetAddress.getByName(endpoint)));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public String getServerId(String endpoint) {
      try {
         return this.getServerId(InetAddress.getByName(endpoint));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public String getServerId() {
      return this.getServerId(Addresses.Internode.getBroadcastAddress());
   }

   public Map<String, CoreIndexingStatus> getCoreIndexingStatus(String endpoint) {
      try {
         String address = InetAddress.getByName(endpoint).isLoopbackAddress()?Addresses.Internode.getBroadcastAddress().getHostAddress():endpoint;
         return this.getCoreIndexingStatus(InetAddress.getByName(address));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Boolean getActiveStatus(String endpoint) {
      try {
         return Boolean.valueOf(this.isActive(InetAddress.getByName(endpoint)));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Boolean getBlacklistedStatus(String endpoint) {
      try {
         return Boolean.valueOf(this.getBlacklistedStatus(InetAddress.getByName(endpoint)));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public Double getNodeHealth(String endpoint) {
      try {
         String address = InetAddress.getByName(endpoint).isLoopbackAddress()?Addresses.Internode.getBroadcastAddress().getHostAddress():endpoint;
         return this.getNodeHealth(InetAddress.getByName(address));
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }

   public boolean isBlacklisted() {
      return this.getBlacklistedStatus(Addresses.Internode.getBroadcastAddress());
   }

   public void setBlacklisted(boolean blacklisted) {
      DseState.instance.setBlacklistedStatusAsync(blacklisted);
      logger.info("This node is now {}, and it will participate in distributed search queries{}.", blacklisted?"blacklisted":"non-blacklisted", blacklisted?" only when no active replicas exist":"");
   }

   public boolean vnodesEnabled() {
      return StorageService.instance.getTokenMetadata().sortedTokens().size() != Gossiper.instance.getEndpointStates().size();
   }

   public SortedMap<String, EndpointStateTrackerMXBean.NodeStatus> getRing(String keyspace) throws UnknownHostException {
      Map<String, String> tokenToEndpoint = StorageService.instance.getTokenToEndpointMap();
      List<String> sortedTokens = new ArrayList(tokenToEndpoint.keySet());
      Collection<String> liveNodes = StorageService.instance.getLiveNodes();
      Collection<String> deadNodes = StorageService.instance.getUnreachableNodes();
      Collection<String> joiningNodes = StorageService.instance.getJoiningNodes();
      Collection<String> leavingNodes = StorageService.instance.getLeavingNodes();
      Collection<String> movingNodes = StorageService.instance.getMovingNodes();
      Map<String, String> loadMap = StorageService.instance.getLoadMap();
      LinkedHashMap<String, List<String>> endpointToTokens = this.endpointToTokens(sortedTokens, tokenToEndpoint);
      boolean vNodesEnabled = endpointToTokens.size() != tokenToEndpoint.size();
      boolean effective = true;

      Object ownerships;
      try {
         ownerships = StorageService.instance.effectiveOwnership(keyspace);
      } catch (IllegalStateException var33) {
         effective = false;
         ownerships = StorageService.instance.getOwnership();
      }

      Map<String, String> leaderLabels = this.getLeaderLabels();
      SortedMap<String, EndpointStateTrackerMXBean.NodeStatus> ring = new TreeMap();
      Iterator var16 = endpointToTokens.keySet().iterator();

      while(var16.hasNext()) {
         String primaryEndpoint = (String)var16.next();
         InetAddress ep = InetAddress.getByName(primaryEndpoint);
         String dataCenter = this.unknownIfNull(this.getDatacenter(ep));
         String rack = this.unknownIfNull(DatabaseDescriptor.getEndpointSnitch().getRack(ep));
         boolean live = liveNodes.contains(primaryEndpoint);
         boolean dead = deadNodes.contains(primaryEndpoint);
         String status = live?"Up":(dead?"Down":"?");
         String state = "Normal";
         if(joiningNodes.contains(primaryEndpoint)) {
            state = "Joining";
         } else if(leavingNodes.contains(primaryEndpoint)) {
            state = "Leaving";
         } else if(movingNodes.contains(primaryEndpoint)) {
            state = "Moving";
         }

         String load = loadMap.containsKey(primaryEndpoint)?(String)loadMap.get(primaryEndpoint):"?";
         String owns = (new DecimalFormat("##0.00%")).format(((Map)ownerships).get(ep) == null?0.0D:(double)((Float)((Map)ownerships).get(ep)).floatValue());
         String legacyWorkloadName = this.unknownIfNull(this.getLegacyWorkload(ep));
         boolean isGraph = this.getIsGraphServer(ep);
         String analyticsLabel = "";
         if(leaderLabels.containsKey(primaryEndpoint)) {
            analyticsLabel = (String)leaderLabels.get(primaryEndpoint);
         } else if(Workload.Analytics.name().equals(legacyWorkloadName)) {
            analyticsLabel = "(SW)";
         }

         List<String> tokens = (List)endpointToTokens.get(primaryEndpoint);
         String token = vNodesEnabled?Integer.toString(tokens.size()):(String)tokens.get(0);
         String serverId = this.unknownIfNull(this.getServerId(ep));
         ring.put(primaryEndpoint, new EndpointStateTrackerMXBean.NodeStatus(primaryEndpoint, dataCenter, rack, legacyWorkloadName, status, state, load, owns, token, effective, serverId, isGraph, this.getNodeHealth(primaryEndpoint).doubleValue(), analyticsLabel));
      }

      return ring;
   }

   private String unknownIfNull(String value) {
      return value == null?Workload.Unknown.name():value;
   }

   private Map<String, String> getLeaderLabels() {
      try {
         return (Map)((LeasePlugin)DseInjector.get().getProvider(LeasePlugin.class).get()).getAllLeases().stream().filter((row) -> {
            return row.name.equals("Leader/master/6.0") && row.holder != null;
         }).collect(Collectors.toMap((row) -> {
            return row.holder.getHostAddress();
         }, (row) -> {
            return "(SM)";
         }));
      } catch (Exception var2) {
         logger.debug("Lease exception: ", var2);
         return Collections.emptyMap();
      }
   }

   private LinkedHashMap<String, List<String>> endpointToTokens(List<String> sortedTokens, Map<String, String> tokenToEndpoint) {
      LinkedHashMap<String, List<String>> result = new LinkedHashMap();

      String token;
      Object tokenList;
      for(Iterator var4 = sortedTokens.iterator(); var4.hasNext(); ((List)tokenList).add(token)) {
         token = (String)var4.next();
         String endpoint = (String)tokenToEndpoint.get(token);
         tokenList = (List)result.get(endpoint);
         if(tokenList == null) {
            tokenList = new ArrayList();
            result.put(endpoint, tokenList);
         }
      }

      return result;
   }
}
