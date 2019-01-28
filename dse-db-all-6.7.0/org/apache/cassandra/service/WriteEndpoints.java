package org.apache.cassandra.service;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.view.ViewUtils;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public class WriteEndpoints implements Iterable<InetAddress> {
   private final Keyspace keyspace;
   private final List<InetAddress> natural;
   private final List<InetAddress> pending;
   private final List<InetAddress> live;
   private final List<InetAddress> dead;

   private WriteEndpoints(Keyspace keyspace, List<InetAddress> natural, List<InetAddress> pending, List<InetAddress> live, List<InetAddress> dead) {
      this.keyspace = keyspace;
      this.natural = natural;
      this.pending = pending;
      this.live = live;
      this.dead = dead;
   }

   private WriteEndpoints(Keyspace keyspace, List<InetAddress> natural, List<InetAddress> pending) {
      this.keyspace = keyspace;
      this.natural = natural;
      this.pending = pending;
      UnmodifiableArrayList.Builder<InetAddress> liveBuilder = UnmodifiableArrayList.builder(natural.size() + pending.size());
      UnmodifiableArrayList.Builder<InetAddress> deadBuilder = null;
      deadBuilder = filterEndpoints(natural, liveBuilder, deadBuilder);
      deadBuilder = filterEndpoints(pending, liveBuilder, deadBuilder);
      this.live = liveBuilder.build();
      this.dead = deadBuilder == null?UnmodifiableArrayList.emptyList():deadBuilder.build();
   }

   private static UnmodifiableArrayList.Builder<InetAddress> filterEndpoints(List<InetAddress> endpoints, UnmodifiableArrayList.Builder<InetAddress> liveBuilder, UnmodifiableArrayList.Builder<InetAddress> deadBuilder) {
      for(int i = 0; i < endpoints.size(); ++i) {
         InetAddress endpoint = (InetAddress)endpoints.get(i);
         if(FailureDetector.instance.isAlive(endpoint)) {
            liveBuilder.add(endpoint);
         } else {
            if(deadBuilder == null) {
               deadBuilder = UnmodifiableArrayList.builder(2);
            }

            deadBuilder.add(endpoint);
         }
      }

      return deadBuilder;
   }

   public static WriteEndpoints compute(String keyspace, DecoratedKey partitionKey) {
      Token tk = partitionKey.getToken();
      List<InetAddress> natural = StorageService.instance.getNaturalEndpoints((String)keyspace, (RingPosition)tk);
      List<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace);
      return new WriteEndpoints(Keyspace.open(keyspace), natural, pending);
   }

   public static WriteEndpoints compute(IMutation mutation) {
      return compute(mutation.getKeyspaceName(), mutation.key());
   }

   public static WriteEndpoints compute(Commit commit) {
      return compute(commit.update.metadata().keyspace, commit.update.partitionKey());
   }

   public static WriteEndpoints withLive(Keyspace keyspace, Iterable<InetAddress> live) {
      List<InetAddress> copy = UnmodifiableArrayList.copyOf(live);
      return new WriteEndpoints(keyspace, copy, UnmodifiableArrayList.emptyList(), copy, UnmodifiableArrayList.emptyList());
   }

   public static WriteEndpoints computeForView(Token baseToken, Mutation mutation) {
      String keyspace = mutation.getKeyspaceName();
      Token tk = mutation.key().getToken();
      Optional<InetAddress> pairedEndpoint = ViewUtils.getViewNaturalEndpoint(keyspace, baseToken, tk);
      List<InetAddress> natural = pairedEndpoint.isPresent()?UnmodifiableArrayList.of(pairedEndpoint.get()):UnmodifiableArrayList.emptyList();
      List<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace);
      return new WriteEndpoints(Keyspace.open(keyspace), natural, pending);
   }

   public Keyspace keyspace() {
      return this.keyspace;
   }

   public WriteEndpoints restrictToLocalDC() {
      Predicate<InetAddress> isLocalDc = (host) -> {
         return DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(host);
      };
      return new WriteEndpoints(this.keyspace, UnmodifiableArrayList.copyOf(Iterables.filter(this.natural, isLocalDc)), this.pending.isEmpty()?UnmodifiableArrayList.emptyList():UnmodifiableArrayList.copyOf(Iterables.filter(this.pending, isLocalDc)));
   }

   public WriteEndpoints withoutLocalhost(boolean requireLocalhost) {
      InetAddress localhost = FBUtilities.getBroadcastAddress();
      if(requireLocalhost && this.natural.size() == 1 && this.pending.isEmpty()) {
         assert ((InetAddress)this.natural.get(0)).equals(localhost);

         return new WriteEndpoints(this.keyspace, UnmodifiableArrayList.emptyList(), UnmodifiableArrayList.emptyList(), UnmodifiableArrayList.emptyList(), UnmodifiableArrayList.emptyList());
      } else {
         Predicate<InetAddress> notLocalhost = (host) -> {
            return !host.equals(localhost);
         };
         List<InetAddress> newNatural = new ArrayList(this.natural.size() - 1);
         Iterables.addAll(newNatural, Iterables.filter(this.natural, notLocalhost));
         List<InetAddress> newLive = new ArrayList(this.live.size() - 1);
         Iterables.addAll(newLive, Iterables.filter(this.live, notLocalhost));
         return new WriteEndpoints(this.keyspace, newNatural, this.pending, newLive, this.dead);
      }
   }

   public void checkAvailability(ConsistencyLevel consistency) throws UnavailableException {
      consistency.assureSufficientLiveNodes(this.keyspace, this.live);
   }

   public boolean isEmpty() {
      return this.count() == 0;
   }

   public int count() {
      return this.natural.size() + this.pending.size();
   }

   public int naturalCount() {
      return this.natural.size();
   }

   public int pendingCount() {
      return this.pending.size();
   }

   public int liveCount() {
      return this.live.size();
   }

   public Iterator<InetAddress> iterator() {
      return Iterators.concat(this.natural.iterator(), this.pending.iterator());
   }

   public List<InetAddress> natural() {
      return this.natural;
   }

   public List<InetAddress> pending() {
      return this.pending;
   }

   public List<InetAddress> live() {
      return this.live;
   }

   public List<InetAddress> dead() {
      return this.dead;
   }

   public String toString() {
      return this.live.toString();
   }
}
