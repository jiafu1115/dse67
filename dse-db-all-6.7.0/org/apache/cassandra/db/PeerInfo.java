package org.apache.cassandra.db;

import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.bdp.snitch.Workload;
import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.UntypedResultSet;

public class PeerInfo {
   @Nullable
   String rack;
   @Nullable
   String dc;
   @Nullable
   ProductVersion.Version version;
   @Nullable
   ProductVersion.Version dseVersion;
   @Nullable
   UUID schemaVersion;
   @Nullable
   UUID hostId;
   @Nullable
   InetAddress preferredIp;
   @Nullable
   Set<Workload> workloads;
   @Nullable
   Boolean graph;
   @Nullable
   String serverId;

   PeerInfo() {
   }

   PeerInfo(UntypedResultSet.Row row) {
      if(row != null) {
         this.rack = row.has("rack")?row.getString("rack"):null;
         this.dc = row.has("data_center")?row.getString("data_center"):null;
         this.version = row.has("release_version")?new ProductVersion.Version(row.getString("release_version")):null;
         this.dseVersion = row.has("dse_version")?new ProductVersion.Version(row.getString("dse_version")):null;
         this.schemaVersion = row.has("schema_version")?row.getUUID("schema_version"):null;
         this.hostId = row.has("host_id")?row.getUUID("host_id"):null;
         this.preferredIp = row.has("preferred_ip")?row.getInetAddress("preferred_ip"):null;
         this.workloads = row.has("workloads")?Workload.fromString(row.getString("workloads")):null;
         this.graph = row.has("graph")?Boolean.valueOf(row.getString("graph")):null;
         this.serverId = row.has("server_id")?row.getString("server_id"):null;
      }
   }

   PeerInfo setRack(String rack) {
      this.rack = rack;
      return this;
   }

   PeerInfo setDc(String dc) {
      this.dc = dc;
      return this;
   }

   PeerInfo setVersion(ProductVersion.Version version) {
      this.version = version;
      return this;
   }

   PeerInfo setDseVersion(ProductVersion.Version dseVersion) {
      this.dseVersion = dseVersion;
      return this;
   }

   PeerInfo setSchemaVersion(UUID schemaVersion) {
      this.schemaVersion = schemaVersion;
      return this;
   }

   PeerInfo setHostId(UUID hostId) {
      this.hostId = hostId;
      return this;
   }

   PeerInfo setPreferredIp(InetAddress preferredIp) {
      this.preferredIp = preferredIp;
      return this;
   }

   PeerInfo setWorkloads(Set<Workload> workloads) {
      this.workloads = workloads;
      return this;
   }

   PeerInfo setIsGraphNode(Boolean isGraphNode) {
      this.graph = isGraphNode;
      return this;
   }

   PeerInfo setServerId(String serverId) {
      this.serverId = serverId;
      return this;
   }

   PeerInfo setValue(String name, Object value) {
      byte var4 = -1;
      switch(name.hashCode()) {
      case -1801402742:
         if(name.equals("data_center")) {
            var4 = 1;
         }
         break;
      case -197437545:
         if(name.equals("server_id")) {
            var4 = 9;
         }
         break;
      case 3492567:
         if(name.equals("rack")) {
            var4 = 0;
         }
         break;
      case 98615630:
         if(name.equals("graph")) {
            var4 = 8;
         }
         break;
      case 114832207:
         if(name.equals("dse_version")) {
            var4 = 3;
         }
         break;
      case 280818848:
         if(name.equals("release_version")) {
            var4 = 2;
         }
         break;
      case 1098693394:
         if(name.equals("host_id")) {
            var4 = 5;
         }
         break;
      case 1102369756:
         if(name.equals("workloads")) {
            var4 = 7;
         }
         break;
      case 1684719674:
         if(name.equals("schema_version")) {
            var4 = 4;
         }
         break;
      case 1920043429:
         if(name.equals("preferred_ip")) {
            var4 = 6;
         }
      }

      switch(var4) {
      case 0:
         return this.setRack((String)value);
      case 1:
         return this.setDc((String)value);
      case 2:
         return this.setVersion(value != null?new ProductVersion.Version((String)value):null);
      case 3:
         return this.setDseVersion(value != null?new ProductVersion.Version((String)value):null);
      case 4:
         return this.setSchemaVersion((UUID)value);
      case 5:
         return this.setHostId((UUID)value);
      case 6:
         return this.setPreferredIp((InetAddress)value);
      case 7:
         return this.setWorkloads(value != null?Workload.fromStringSet((Set)value):null);
      case 8:
         return this.setIsGraphNode(value != null?(Boolean)value:null);
      case 9:
         return this.setServerId((String)value);
      default:
         return this;
      }
   }

   @Nullable
   public String getRack() {
      return this.rack;
   }

   @Nullable
   public String getDc() {
      return this.dc;
   }

   @Nullable
   public ProductVersion.Version getVersion() {
      return this.version;
   }

   @Nullable
   public ProductVersion.Version getDseVersion() {
      return this.dseVersion;
   }

   @Nullable
   public UUID getSchemaVersion() {
      return this.schemaVersion;
   }

   @Nullable
   public UUID getHostId() {
      return this.hostId;
   }

   @Nullable
   public InetAddress getPreferredIp() {
      return this.preferredIp;
   }

   @Nullable
   public Set<Workload> getWorkloads() {
      return this.workloads;
   }

   @Nullable
   public Boolean getGraph() {
      return this.graph;
   }

   @Nullable
   public String getServerId() {
      return this.serverId;
   }

   public String toString() {
      return "PeerInfo{preferredIp=" + this.preferredIp + ", dc='" + this.dc + '\'' + ", rack='" + this.rack + '\'' + ", workloads=" + this.workloads + ", version=" + this.version + ", dseVersion=" + this.dseVersion + ", schemaVersion=" + this.schemaVersion + ", hostId=" + this.hostId + ", graph=" + this.graph + ", serverId='" + this.serverId + '\'' + '}';
   }
}
