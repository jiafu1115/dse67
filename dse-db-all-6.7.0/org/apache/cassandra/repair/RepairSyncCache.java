package org.apache.cassandra.repair;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.RangeHash;
import org.apache.cassandra.utils.SetsFactory;

public class RepairSyncCache {
   private final Map<InetAddress, Set<Range<Token>>> skippedSource;
   private final Map<InetAddress, Set<RangeHash>> received;

   @VisibleForTesting
   public RepairSyncCache() {
      this(Collections.emptyMap());
   }

   public RepairSyncCache(Map<InetAddress, Set<Range<Token>>> skippedSource) {
      this.received = new HashMap();
      this.skippedSource = skippedSource;
   }

   public boolean shouldSkip(InetAddress source, InetAddress destination, RangeHash rangeHash) {
      return this.isSkipped(source, rangeHash.range)?true:this.get(destination).contains(rangeHash);
   }

   public void add(InetAddress endpoint, RangeHash rangeHash) {
      this.get(endpoint).add(rangeHash);
   }

   @VisibleForTesting
   protected Set<RangeHash> get(InetAddress endpoint) {
      return (Set)this.received.computeIfAbsent(endpoint, (i) -> {
         return SetsFactory.newSet();
      });
   }

   private boolean isSkipped(InetAddress endpoint, Range<Token> range) {
      return ((Set)this.skippedSource.getOrDefault(endpoint, Collections.emptySet())).stream().anyMatch((r) -> {
         return r.contains((AbstractBounds)range);
      });
   }
}
