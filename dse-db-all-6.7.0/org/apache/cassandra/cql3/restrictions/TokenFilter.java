package org.apache.cassandra.cql3.restrictions;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.ImmutableRangeSet.Builder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SetsFactory;

abstract class TokenFilter implements PartitionKeyRestrictions {
   final PartitionKeyRestrictions restrictions;
   final TokenRestriction tokenRestriction;
   private final IPartitioner partitioner;

   static TokenFilter create(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction) {
      boolean onToken = restrictions.needFiltering(tokenRestriction.metadata) || restrictions.size() < tokenRestriction.size();
      return (TokenFilter)(onToken?new TokenFilter.OnToken(restrictions, tokenRestriction):new TokenFilter.NotOnToken(restrictions, tokenRestriction));
   }

   private TokenFilter(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction) {
      this.restrictions = restrictions;
      this.tokenRestriction = tokenRestriction;
      this.partitioner = tokenRestriction.metadata.partitioner;
   }

   public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
      Set<Restriction> set = SetsFactory.newSet();
      set.addAll(this.restrictions.getRestrictions(columnDef));
      set.addAll(this.tokenRestriction.getRestrictions(columnDef));
      return set;
   }

   public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException {
      return this.filter(this.restrictions.values(options), options);
   }

   public PartitionKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException {
      return restriction.isOnToken()?create(this.restrictions, (TokenRestriction)this.tokenRestriction.mergeWith(restriction)):create(this.restrictions.mergeWith(restriction), this.tokenRestriction);
   }

   private List<ByteBuffer> filter(List<ByteBuffer> values, QueryOptions options) throws InvalidRequestException {
      RangeSet<Token> rangeSet = this.tokenRestriction.hasSlice()?this.toRangeSet(this.tokenRestriction, options):this.toRangeSet(this.tokenRestriction.values(options));
      return this.filterWithRangeSet(rangeSet, values);
   }

   private List<ByteBuffer> filterWithRangeSet(RangeSet<Token> tokens, List<ByteBuffer> values) {
      List<ByteBuffer> remaining = new ArrayList();
      Iterator var4 = values.iterator();

      while(var4.hasNext()) {
         ByteBuffer value = (ByteBuffer)var4.next();
         Token token = this.partitioner.getToken(value);
         if(tokens.contains(token)) {
            remaining.add(value);
         }
      }

      return remaining;
   }

   private RangeSet<Token> toRangeSet(List<ByteBuffer> buffers) {
      Builder<Token> builder = ImmutableRangeSet.builder();
      Iterator var3 = buffers.iterator();

      while(var3.hasNext()) {
         ByteBuffer buffer = (ByteBuffer)var3.next();
         builder.add(Range.singleton(this.deserializeToken(buffer)));
      }

      return builder.build();
   }

   private RangeSet<Token> toRangeSet(TokenRestriction slice, QueryOptions options) throws InvalidRequestException {
      Token start;
      if(slice.hasBound(Bound.START)) {
         start = this.deserializeToken((ByteBuffer)slice.bounds(Bound.START, options).get(0));
         BoundType startBoundType = toBoundType(slice.isInclusive(Bound.START));
         if(!slice.hasBound(Bound.END)) {
            return ImmutableRangeSet.of(Range.downTo(start, startBoundType));
         } else {
            BoundType endBoundType = toBoundType(slice.isInclusive(Bound.END));
            Token end = this.deserializeToken((ByteBuffer)slice.bounds(Bound.END, options).get(0));
            return start.equals(end) && (BoundType.OPEN == startBoundType || BoundType.OPEN == endBoundType)?ImmutableRangeSet.of():(start.compareTo(end) <= 0?ImmutableRangeSet.of(Range.range(start, startBoundType, end, endBoundType)):ImmutableRangeSet.<Token>builder().add(Range.upTo(end, endBoundType)).add(Range.downTo(start, startBoundType)).build());
         }
      } else {
         start = this.deserializeToken((ByteBuffer)slice.bounds(Bound.END, options).get(0));
         return ImmutableRangeSet.of(Range.upTo(start, toBoundType(slice.isInclusive(Bound.END))));
      }
   }

   private Token deserializeToken(ByteBuffer buffer) {
      return this.partitioner.getTokenFactory().fromByteArray(buffer);
   }

   private static BoundType toBoundType(boolean inclusive) {
      return inclusive?BoundType.CLOSED:BoundType.OPEN;
   }

   public ColumnMetadata getFirstColumn() {
      return this.restrictions.getFirstColumn();
   }

   public ColumnMetadata getLastColumn() {
      return this.restrictions.getLastColumn();
   }

   public List<ColumnMetadata> getColumnDefs() {
      return this.restrictions.getColumnDefs();
   }

   public void addFunctionsTo(List<Function> functions) {
      this.restrictions.addFunctionsTo(functions);
   }

   public void forEachFunction(Consumer<Function> consumer) {
      this.restrictions.forEachFunction(consumer);
   }

   public boolean hasSupportingIndex(IndexRegistry indexRegistry) {
      return this.restrictions.hasSupportingIndex(indexRegistry);
   }

   public void addRowFilterTo(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      this.restrictions.addRowFilterTo(filter, indexRegistry, options);
   }

   public boolean isEmpty() {
      return this.restrictions.isEmpty();
   }

   public int size() {
      return this.restrictions.size();
   }

   public boolean needFiltering(TableMetadata table) {
      return this.restrictions.needFiltering(table);
   }

   public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table) {
      return this.restrictions.hasUnrestrictedPartitionKeyComponents(table);
   }

   public boolean hasSlice() {
      return this.restrictions.hasSlice();
   }

   private static final class NotOnToken extends TokenFilter {
      private NotOnToken(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction) {
         super(restrictions, tokenRestriction);
      }

      public boolean isInclusive(Bound bound) {
         return this.restrictions.isInclusive(bound);
      }

      public boolean hasBound(Bound bound) {
         return this.restrictions.hasBound(bound);
      }

      public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException {
         return this.restrictions.bounds(bound, options);
      }

      public boolean hasIN() {
         return this.restrictions.hasIN();
      }

      public boolean hasContains() {
         return this.restrictions.hasContains();
      }

      public boolean hasOnlyEqualityRestrictions() {
         return this.restrictions.hasOnlyEqualityRestrictions();
      }
   }

   private static final class OnToken extends TokenFilter {
      private OnToken(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction) {
         super(restrictions, tokenRestriction);
      }

      public boolean isOnToken() {
         return true;
      }

      public boolean isInclusive(Bound bound) {
         return this.tokenRestriction.isInclusive(bound);
      }

      public boolean hasBound(Bound bound) {
         return this.tokenRestriction.hasBound(bound);
      }

      public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException {
         return this.tokenRestriction.bounds(bound, options);
      }
   }
}
