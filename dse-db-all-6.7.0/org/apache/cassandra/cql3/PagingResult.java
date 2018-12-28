package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class PagingResult {
   public static final PagingResult NONE = new PagingResult((ByteBuffer)null);
   public final ByteBuffer state;
   public final int seqNo;
   public final boolean last;

   public PagingResult(ByteBuffer pagingState) {
      this(pagingState, -1, true);
   }

   public PagingResult(ByteBuffer state, int seqNo, boolean last) {
      this.state = state;
      this.seqNo = seqNo;
      this.last = last;
   }

   public boolean equals(Object other) {
      if(this == other) {
         return true;
      } else if(!(other instanceof PagingResult)) {
         return false;
      } else {
         PagingResult that = (PagingResult)other;
         return Objects.equals(this.state, that.state) && this.seqNo == that.seqNo && this.last == that.last;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.state, Integer.valueOf(this.seqNo), Boolean.valueOf(this.last)});
   }

   public String toString() {
      return String.format("[Page seq. no. %d%s - state %s]", new Object[]{Integer.valueOf(this.seqNo), this.last?" final":"", this.state});
   }
}
