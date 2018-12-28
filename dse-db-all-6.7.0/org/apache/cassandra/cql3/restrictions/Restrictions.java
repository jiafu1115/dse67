package org.apache.cassandra.cql3.restrictions;

import java.util.Set;
import org.apache.cassandra.schema.ColumnMetadata;

public interface Restrictions extends Restriction {
   Set<Restriction> getRestrictions(ColumnMetadata var1);

   boolean isEmpty();

   int size();

   default boolean hasIN() {
      return false;
   }

   default boolean hasContains() {
      return false;
   }

   default boolean hasSlice() {
      return false;
   }

   default boolean hasOnlyEqualityRestrictions() {
      return true;
   }
}
