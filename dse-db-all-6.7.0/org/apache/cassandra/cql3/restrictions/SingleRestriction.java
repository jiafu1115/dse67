package org.apache.cassandra.cql3.restrictions;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;

public abstract class SingleRestriction implements Restriction {
   protected static final int flagSlice = 1;
   protected static final int flagEQ = 2;
   protected static final int flagLIKE = 4;
   protected static final int flagIN = 8;
   protected static final int flagContains = 16;
   protected static final int flagNotNull = 32;
   protected static final int flagMultiColumn = 64;
   private final int flags;

   protected SingleRestriction(int flags) {
      this.flags = flags;
   }

   public final boolean isSlice() {
      return (this.flags & 1) != 0;
   }

   public final boolean isEQ() {
      return (this.flags & 2) != 0;
   }

   public final boolean isLIKE() {
      return (this.flags & 4) != 0;
   }

   public final boolean isIN() {
      return (this.flags & 8) != 0;
   }

   public final boolean isContains() {
      return (this.flags & 16) != 0;
   }

   public final boolean isNotNull() {
      return (this.flags & 32) != 0;
   }

   public final boolean isMultiColumn() {
      return (this.flags & 64) != 0;
   }

   public boolean hasBound(Bound b) {
      return true;
   }

   public boolean isInclusive(Bound b) {
      return true;
   }

   public abstract SingleRestriction mergeWith(SingleRestriction var1);

   public abstract MultiCBuilder appendTo(MultiCBuilder var1, QueryOptions var2);

   public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options) {
      return this.appendTo(builder, options);
   }
}
