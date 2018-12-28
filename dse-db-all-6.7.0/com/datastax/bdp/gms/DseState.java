package com.datastax.bdp.gms;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.bdp.server.CoreSystemInfo;
import com.datastax.bdp.server.ServerId;
import com.datastax.bdp.snitch.Workload;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SetsFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseState {
   public static final DseState instance = new DseState();
   private static final String INDEXING_CORES = "indexingCores";
   private static final String CORE_INDEXING_STATUS = "coreIndexingStatus";
   private static final String WORKLOAD = "workload";
   private static final String WORKLOADS = "workloads";
   private static final String ACTIVE = "active";
   private static final String BLACKLISTED = "blacklisted";
   private static final String NODE_HEALTH = "health";
   private static final String DSE_VERSION = "dse_version";
   private static final String GRAPH = "graph";
   private static final String SERVER_ID = "server_id";
   private static final Logger logger = LoggerFactory.getLogger(DseState.class);
   private static final ObjectMapper jsonMapper = new ObjectMapper();
   private final ExecutorService gossipStateUpdater = Executors.newSingleThreadExecutor((new ThreadFactoryBuilder()).setDaemon(true).setNameFormat("DseGossipStateUpdater").build());

   private DseState() {
   }

   public static VersionedValue initialLocalApplicationState() {
      try {
         Set<Workload> workloads = CoreSystemInfo.getWorkloads();
         String workloadNames = Workload.workloadNames(workloads);
         Map<String, Object> state = doGetCurrentState();
         state.put("workload", Workload.legacyWorkloadName(workloads));
         state.put("workloads", workloadNames);
         state.put("graph", Boolean.valueOf(CoreSystemInfo.isGraphNode()));
         state.put("dse_version", ProductVersion.getDSEVersionString());
         state.put("server_id", ServerId.getServerId());
         state.put("active", Boolean.valueOf(false));
         String newValue = jsonMapper.writeValueAsString(state);
         return StorageService.instance.valueFactory.datacenter(newValue);
      } catch (IOException var4) {
         throw new RuntimeException(var4);
      }
   }

   public Future<?> setIndexingStatusAsync(String core, DseState.CoreIndexingStatus status) {
      return this.gossipStateUpdater.submit(() -> {
         this.setIndexingStatusSync(core, status);
      });
   }

   public synchronized void setIndexingStatusSync(String core, DseState.CoreIndexingStatus status) {
      try {
         Map<String, Object> state = doGetCurrentState();
         Map<String, String> indexingStatus = doGetCoreIndexingStatus(state);
         switch(null.$SwitchMap$com$datastax$bdp$gms$DseState$CoreIndexingStatus[status.ordinal()]) {
         case 1:
         case 2:
            indexingStatus.put(core, status.toString());
            break;
         case 3:
            indexingStatus.remove(core);
         }

         state.put("coreIndexingStatus", indexingStatus);
         Set<String> indexingCores = new HashSet();
         Iterator var6 = indexingStatus.entrySet().iterator();

         while(var6.hasNext()) {
            Entry<String, String> entry = (Entry)var6.next();
            if(((String)entry.getValue()).equals(DseState.CoreIndexingStatus.INDEXING.toString())) {
               indexingCores.add(entry.getKey());
            }
         }

         state.put("indexingCores", indexingCores);
         String newValue = jsonMapper.writeValueAsString(state);
         Gossiper.instance.updateLocalApplicationState(ApplicationState.DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
      } catch (IOException var8) {
         throw new RuntimeException(var8);
      }
   }

   public Future<?> setActiveStatusAsync(boolean active) {
      return this.gossipStateUpdater.submit(() -> {
         this.setActiveStatusSync(active);
      });
   }

   public synchronized void setActiveStatusSync(boolean active) {
      setBooleanApplicationState("active", active);
   }

   public Future<?> setBlacklistedStatusAsync(boolean blacklisted) {
      return this.gossipStateUpdater.submit(() -> {
         this.setBlacklistedStatusSync(blacklisted);
      });
   }

   public synchronized void setBlacklistedStatusSync(boolean blacklisted) {
      setBooleanApplicationState("blacklisted", blacklisted);
   }

   public void setNodeHealthAsync(double nodeHealth) {
      Preconditions.checkArgument(nodeHealth >= 0.0D && nodeHealth <= 1.0D, "Node Health cannot be less 0.0 or more than 1.0");
      this.gossipStateUpdater.submit(() -> {
         this.setNodeHealthSync(nodeHealth);
      });
   }

   private synchronized void setNodeHealthSync(double nodeHealth) {
      try {
         Map<String, Object> state = doGetCurrentState();
         state.put("health", Double.valueOf(nodeHealth));
         String newValue = jsonMapper.writeValueAsString(state);
         Gossiper.instance.updateLocalApplicationState(ApplicationState.DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
      } catch (Throwable var5) {
         logger.error("Failed to set the node health", var5);
      }

   }

   public static String getServerID(Map<String, Object> values) {
      Object id = values.get("server_id");
      return id == null?null:id.toString();
   }

   public static Map<String, Object> getValues(ApplicationState applicationState, VersionedValue value) {
      return applicationState.equals(ApplicationState.DSE_GOSSIP_STATE)?getValues(value):null;
   }

   public static Map<String, Object> getValues(VersionedValue value) {
      try {
         return (Map)jsonMapper.readValue(value.value, Map.class);
      } catch (IOException var2) {
         throw new RuntimeException(var2);
      }
   }

   public static Set<Workload> getWorkloads(Map<String, Object> values, InetAddress endpoint) {
      if(null != values.get("workloads")) {
         return Workload.fromString((String)values.get("workloads"), endpoint);
      } else {
         Boolean graphEnabled = getIsGraphNode(values);
         if(null == graphEnabled) {
            graphEnabled = Boolean.valueOf(false);
            logger.warn(String.format("Node %s did not send a value for the %s field.", new Object[]{endpoint, "graph"}));
         }

         return Workload.fromLegacyWorkloadName((String)values.get("workload"), graphEnabled.booleanValue());
      }
   }

   public static Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(Map<String, Object> values) {
      Map<String, DseState.CoreIndexingStatus> coreIndexingStatus = new HashMap();
      Map<String, String> wireFormat = doGetCoreIndexingStatus(values);
      Iterator var3 = wireFormat.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<String, String> entry = (Entry)var3.next();
         coreIndexingStatus.put(entry.getKey(), DseState.CoreIndexingStatus.valueOf((String)entry.getValue()));
      }

      return coreIndexingStatus;
   }

   public static boolean getActiveStatus(Map<String, Object> values) {
      return getBoolean("active", values);
   }

   public static boolean getBlacklistingStatus(Map<String, Object> values) {
      return getBoolean("blacklisted", values);
   }

   public static ProductVersion.Version getDseVersion(EndpointState state) {
      Map<String, Object> values = doGetState(state);
      return getDseVersion(values);
   }

   public static ProductVersion.Version getDseVersion(Map<String, Object> values) {
      if(values == null) {
         return null;
      } else {
         Object version = values.get("dse_version");
         return version == null?null:new ProductVersion.Version(version.toString());
      }
   }

   public static Boolean getIsGraphNode(Map<String, Object> values) {
      Object value = values.get("graph");
      return value == null?null:Boolean.valueOf(Boolean.parseBoolean(value.toString()));
   }

   public static double getNodeHealth(Map<String, Object> values) {
      Object nodeHealth = values.get("health");
      return nodeHealth != null?((Double)nodeHealth).doubleValue():0.0D;
   }

   private static void setBooleanApplicationState(String key, boolean value) {
      try {
         Map<String, Object> state = doGetCurrentState();
         state.put(key, Boolean.toString(value));
         String newValue = jsonMapper.writeValueAsString(state);
         Gossiper.instance.updateLocalApplicationState(ApplicationState.DSE_GOSSIP_STATE, StorageService.instance.valueFactory.datacenter(newValue));
      } catch (IOException var4) {
         throw new RuntimeException(var4);
      }
   }

   private static boolean getBoolean(String key, Map<String, Object> values) {
      Object value = values.get(key);
      value = value != null?value:Boolean.FALSE;
      return Boolean.parseBoolean(value.toString());
   }

   private static Map<String, Object> doGetCurrentState() {
      EndpointState currentState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
      return doGetState(currentState);
   }

   private static Map<String, Object> doGetState(EndpointState currentState) {
      if(currentState != null) {
         VersionedValue currentValue = currentState.getApplicationState(ApplicationState.DSE_GOSSIP_STATE);
         if(currentValue != null) {
            return getValues(currentValue);
         }
      }

      return new HashMap();
   }

   private static Map<String, String> doGetCoreIndexingStatus(Map<String, Object> values) {
      Map<String, String> coreIndexingStatus = (Map)getFromValues(values, "coreIndexingStatus");
      if(coreIndexingStatus == null) {
         coreIndexingStatus = new HashMap();
      }

      Set<String> indexingCore = doGetIndexingCores(values);
      Iterator var3 = indexingCore.iterator();

      while(var3.hasNext()) {
         String core = (String)var3.next();
         ((Map)coreIndexingStatus).put(core, DseState.CoreIndexingStatus.INDEXING.toString());
      }

      return (Map)coreIndexingStatus;
   }

   /** @deprecated */
   @Deprecated
   private static Set<String> doGetIndexingCores(Map<String, Object> values) {
      Collection<String> cores = (Collection)getFromValues(values, "indexingCores");
      return cores != null?SetsFactory.setFromCollection(cores):SetsFactory.newSet();
   }

   private static <T> T getFromValues(Map<String, Object> values, String key) {
      return values.get(key);
   }

   public static enum CoreIndexingStatus {
      INDEXING,
      FINISHED,
      FAILED;

      private CoreIndexingStatus() {
      }
   }
}
