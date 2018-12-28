package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.UnmodifiableArrayList;

final class PartitionKeySingleRestrictionSet extends RestrictionSetWrapper implements PartitionKeyRestrictions {
   protected final ClusteringComparator comparator;

   private PartitionKeySingleRestrictionSet(RestrictionSet restrictionSet, ClusteringComparator comparator) {
      super(restrictionSet);
      this.comparator = comparator;
   }

   public PartitionKeyRestrictions mergeWith(Restriction restriction) {
      if(restriction.isOnToken()) {
         return (PartitionKeyRestrictions)(this.isEmpty()?(PartitionKeyRestrictions)restriction:TokenFilter.create(this, (TokenRestriction)restriction));
      } else {
         PartitionKeySingleRestrictionSet.Builder builder = builder(this.comparator);
         List<SingleRestriction> restrictions = this.restrictions();

         for(int i = 0; i < restrictions.size(); ++i) {
            SingleRestriction r = (SingleRestriction)restrictions.get(i);
            builder.addRestriction(r);
         }

         return builder.addRestriction(restriction).build();
      }
   }

   public List<ByteBuffer> values(QueryOptions options) {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN());
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction r = (SingleRestriction)restrictions.get(i);
         r.appendTo(builder, options);
         if(builder.hasMissingElements()) {
            break;
         }
      }

      return builder.buildSerializedPartitionKeys();
   }

   public List<ByteBuffer> bounds(Bound bound, QueryOptions options) {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN());
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction r = (SingleRestriction)restrictions.get(i);
         r.appendBoundTo(builder, bound, options);
         if(builder.hasMissingElements()) {
            return UnmodifiableArrayList.emptyList();
         }
      }

      return builder.buildSerializedPartitionKeys();
   }

   public boolean hasBound(Bound b) {
      return this.isEmpty()?false:this.restrictions.lastRestriction().hasBound(b);
   }

   public boolean isInclusive(Bound b) {
      return this.isEmpty()?false:this.restrictions.lastRestriction().isInclusive(b);
   }

   public void addRowFilterTo(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction r = (SingleRestriction)restrictions.get(i);
         r.addRowFilterTo(filter, indexRegistry, options);
      }

   }

   public boolean needFiltering(TableMetadata table) {
      return this.isEmpty()?false:this.hasUnrestrictedPartitionKeyComponents(table) || this.hasSlice() || this.hasContains();
   }

   public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table) {
      return this.size() < table.partitionKeyColumns().size();
   }

   public static PartitionKeySingleRestrictionSet.Builder builder(ClusteringComparator clusteringComparator) {
      return new PartitionKeySingleRestrictionSet.Builder(clusteringComparator);
   }

   public static final class Builder {
      private final ClusteringComparator clusteringComparator;
      private final List<Restriction> restrictions;

      private Builder(ClusteringComparator clusteringComparator) {
         this.restrictions = new ArrayList();
         this.clusteringComparator = clusteringComparator;
      }

      public PartitionKeySingleRestrictionSet.Builder addRestriction(Restriction restriction) {
         this.restrictions.add(restriction);
         return this;
      }

      public PartitionKeyRestrictions build() {
         RestrictionSet.Builder restrictionSet = RestrictionSet.builder();

         for(int i = 0; i < this.restrictions.size(); ++i) {
            Restriction restriction = (Restriction)this.restrictions.get(i);
            if(restriction.isOnToken()) {
               return this.buildWithTokens(restrictionSet, i);
            }

            restrictionSet.addRestriction((SingleRestriction)restriction);
         }

         return this.buildPartitionKeyRestrictions(restrictionSet);
      }

      private PartitionKeyRestrictions buildWithTokens(RestrictionSet.Builder restrictionSet, int i) {
         Object merged;
         for(merged = this.buildPartitionKeyRestrictions(restrictionSet); i < this.restrictions.size(); ++i) {
            Restriction restriction = (Restriction)this.restrictions.get(i);
            merged = ((PartitionKeyRestrictions)merged).mergeWith(restriction);
         }

         return (PartitionKeyRestrictions)merged;
      }

      private PartitionKeySingleRestrictionSet buildPartitionKeyRestrictions(RestrictionSet.Builder restrictionSet) {
         return new PartitionKeySingleRestrictionSet(restrictionSet.build(), this.clusteringComparator);
      }
   }
}
