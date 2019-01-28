package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UnmodifiableArrayList;

abstract class MessageTargets {
   private static final InetAddress LOCAL = FBUtilities.getBroadcastAddress();
   private final boolean hasLocal;

   private MessageTargets(boolean hasLocal) {
      this.hasLocal = hasLocal;
   }

   static MessageTargets createSimple(Collection<InetAddress> endpoints) {
      if(endpoints instanceof List) {
         return createSimple((List)endpoints);
      } else {
         List<InetAddress> remotes = new ArrayList(endpoints.size());
         boolean hasLocal = false;
         Iterator var3 = endpoints.iterator();

         while(var3.hasNext()) {
            InetAddress endpoint = (InetAddress)var3.next();
            if(endpoint.equals(LOCAL)) {
               hasLocal = true;
            } else {
               remotes.add(endpoint);
            }
         }

         return new MessageTargets.Simple(hasLocal, remotes);
      }
   }

   private static MessageTargets createSimple(List<InetAddress> endpoints) {
      int size = endpoints.size();

      for(int i = 0; i < size; ++i) {
         if(((InetAddress)endpoints.get(i)).equals(LOCAL)) {
            ArrayList<InetAddress> remotes = new ArrayList(size - 1);

            for(int j = 0; j < size; ++j) {
               if(j != i) {
                  remotes.add(endpoints.get(j));
               }
            }

            return new MessageTargets.Simple(true, remotes);
         }
      }

      return new MessageTargets.Simple(false, endpoints);
   }

   static MessageTargets createWithFowardingForRemoteDCs(List<InetAddress> endpoints, String localDc) {
      boolean hasLocal = false;
      List<InetAddress> localDcRemotes = null;
      Map<String, MessageTargets.WithForwards> remoteDcsRemotes = null;

      for(int i = 0; i < endpoints.size(); ++i) {
         InetAddress endpoint = (InetAddress)endpoints.get(i);
         if(endpoint.equals(LOCAL)) {
            hasLocal = true;
         } else {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            if(localDc.equals(dc)) {
               if(localDcRemotes == null) {
                  localDcRemotes = new ArrayList(4);
               }

               localDcRemotes.add(endpoint);
            } else {
               MessageTargets.WithForwards dcRemotes = remoteDcsRemotes == null?null:(MessageTargets.WithForwards)remoteDcsRemotes.get(dc);
               if(dcRemotes == null) {
                  dcRemotes = new MessageTargets.WithForwards(endpoint);
                  if(remoteDcsRemotes == null) {
                     remoteDcsRemotes = new HashMap();
                  }

                  remoteDcsRemotes.put(dc, dcRemotes);
               } else {
                  dcRemotes.forwards.add(endpoint);
               }
            }
         }
      }

      return new MessageTargets.WithForwarding(hasLocal, (List)(localDcRemotes == null?UnmodifiableArrayList.emptyList():localDcRemotes), (Collection)(remoteDcsRemotes == null?UnmodifiableArrayList.emptyList():remoteDcsRemotes.values()));
   }

   boolean hasLocal() {
      return this.hasLocal;
   }

   abstract boolean hasForwards();

   abstract Iterable<InetAddress> nonForwardingRemotes();

   abstract Iterable<MessageTargets.WithForwards> remotesWithForwards();

   private static class WithForwarding extends MessageTargets {
      private final List<InetAddress> nonForwardingRemotes;
      private final Collection<MessageTargets.WithForwards> remotesWithForwards;

      WithForwarding(boolean hasLocal, List<InetAddress> nonForwardingRemotes, Collection<MessageTargets.WithForwards> remotesWithForwards) {
         super(hasLocal);
         this.nonForwardingRemotes = nonForwardingRemotes;
         this.remotesWithForwards = remotesWithForwards;
      }

      boolean hasForwards() {
         return true;
      }

      Iterable<InetAddress> nonForwardingRemotes() {
         return this.nonForwardingRemotes;
      }

      Iterable<MessageTargets.WithForwards> remotesWithForwards() {
         return this.remotesWithForwards;
      }

      public String toString() {
         return String.format("%slocalDc=%s + remoteDc=%s", new Object[]{this.hasLocal()?"local + ":"", this.nonForwardingRemotes, this.remotesWithForwards});
      }
   }

   private static class Simple extends MessageTargets {
      private final List<InetAddress> remotes;

      Simple(boolean hasLocal, List<InetAddress> remotes) {
         super(hasLocal);
         this.remotes = remotes;
      }

      boolean hasForwards() {
         return false;
      }

      Iterable<InetAddress> nonForwardingRemotes() {
         return this.remotes;
      }

      Iterable<MessageTargets.WithForwards> remotesWithForwards() {
         return UnmodifiableArrayList.emptyList();
      }

      public String toString() {
         return this.hasLocal()?"local + " + this.remotes:this.remotes.toString();
      }
   }

   static class WithForwards {
      final InetAddress target;
      final List<InetAddress> forwards;

      private WithForwards(InetAddress target) {
         this.forwards = new ArrayList(4);
         this.target = target;
      }

      public String toString() {
         return String.format("%s (forwards to: %s)", new Object[]{this.target, this.forwards});
      }
   }
}
