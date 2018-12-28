package org.apache.cassandra.utils;

import org.apache.cassandra.utils.concurrent.Ref;

public class AlwaysPresentFilter implements IFilter {
   public AlwaysPresentFilter() {
   }

   public boolean isPresent(IFilter.FilterKey key) {
      return true;
   }

   public void add(IFilter.FilterKey key) {
   }

   public void clear() {
   }

   public void close() {
   }

   public IFilter sharedCopy() {
      return this;
   }

   public Throwable close(Throwable accumulate) {
      return accumulate;
   }

   public void addTo(Ref.IdentityCollection identities) {
   }

   public long serializedSize() {
      return 0L;
   }

   public long offHeapSize() {
      return 0L;
   }
}
