package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import io.airlift.airline.Option;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.utils.SetsFactory;

public abstract class NodeSyncTracingCommand extends NodeSyncCommand {
   @Option(
      name = {"-n", "--nodes"},
      description = "Comma separated of nodes address on which to act; if omitted, all (live) nodes will be included"
   )
   private String nodeList;

   public NodeSyncTracingCommand() {
   }

   protected Set<InetAddress> nodes(Metadata metadata) {
      Set alive;
      Host h;
      if(this.nodeList == null) {
         alive = SetsFactory.newSet();
         Set<InetAddress> dead = SetsFactory.newSet();
         Iterator var7 = metadata.getAllHosts().iterator();

         while(var7.hasNext()) {
            h = (Host)var7.next();
            if(h.isUp()) {
               alive.add(h.getBroadcastAddress());
            } else {
               dead.add(h.getBroadcastAddress());
            }
         }

         if(!dead.isEmpty()) {
            this.printWarning("Cannot enable tracing on the following dead nodes: %s", new Object[]{dead});
         }

         return alive;
      } else {
         alive = parseInetAddressList(this.nodeList);
         Iterator var3 = alive.iterator();

         InetAddress node;
         do {
            if(!var3.hasNext()) {
               return alive;
            }

            node = (InetAddress)var3.next();
            h = findHost(node, metadata);
            if(h == null) {
               throw new NodeSyncException(node + " does not seem to be a known node in the cluster");
            }
         } while(h.isUp());

         throw new NodeSyncException("Cannot enable tracing on " + node + " as it is currently down");
      }
   }

   boolean isOnAllNodes() {
      return this.nodeList == null;
   }

   private static Host findHost(InetAddress address, Metadata metadata) {
      Iterator var2 = metadata.getAllHosts().iterator();

      Host h;
      do {
         if(!var2.hasNext()) {
            return null;
         }

         h = (Host)var2.next();
      } while(!h.getBroadcastAddress().equals(address));

      return h;
   }
}
