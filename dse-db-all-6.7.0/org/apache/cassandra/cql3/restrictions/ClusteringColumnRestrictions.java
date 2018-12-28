package org.apache.cassandra.cql3.restrictions;

import java.util.List;
import java.util.NavigableSet;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;

final class ClusteringColumnRestrictions extends RestrictionSetWrapper {
   protected final ClusteringComparator comparator;

   private ClusteringColumnRestrictions(ClusteringComparator comparator, RestrictionSet restrictionSet) {
      super(restrictionSet);
      this.comparator = comparator;
   }

   public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN());
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction r = (SingleRestriction)restrictions.get(i);
         r.appendTo(builder, options);
         if(builder.hasMissingElements()) {
            break;
         }
      }

      return builder.build();
   }

   public NavigableSet<ClusteringBound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException {
      MultiCBuilder builder = MultiCBuilder.create(this.comparator, this.hasIN() || this.restrictions.hasMultiColumnSlice());
      int keyPosition = 0;
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction r = (SingleRestriction)restrictions.get(i);
         if(this.handleInFilter(r, keyPosition)) {
            break;
         }

         if(r.isSlice()) {
            r.appendBoundTo(builder, bound, options);
            return builder.buildBoundForSlice(bound.isStart(), r.isInclusive(bound), r.isInclusive(bound.reverse()), r.getColumnDefs());
         }

         r.appendBoundTo(builder, bound, options);
         if(builder.hasMissingElements()) {
            return BTreeSet.empty(this.comparator);
         }

         keyPosition = r.getLastColumn().position() + 1;
      }

      return builder.buildBound(bound.isStart(), true);
   }

   public boolean needFiltering() {
      int position = 0;
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction restriction = (SingleRestriction)restrictions.get(i);
         if(this.handleInFilter(restriction, position)) {
            return true;
         }

         if(!restriction.isSlice()) {
            position = restriction.getLastColumn().position() + 1;
         }
      }

      return this.hasContains();
   }

   public void addRowFilterTo(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) throws InvalidRequestException {
      int position = 0;
      List<SingleRestriction> restrictions = this.restrictions();

      for(int i = 0; i < restrictions.size(); ++i) {
         SingleRestriction restriction = (SingleRestriction)restrictions.get(i);
         if(!this.handleInFilter(restriction, position) && !restriction.hasSupportingIndex(indexRegistry)) {
            if(!restriction.isSlice()) {
               position = restriction.getLastColumn().position() + 1;
            }
         } else {
            restriction.addRowFilterTo(filter, indexRegistry, options);
         }
      }

   }

   private boolean handleInFilter(SingleRestriction restriction, int index) {
      return restriction.isContains() || restriction.isLIKE() || index != restriction.getFirstColumn().position();
   }

   public static ClusteringColumnRestrictions.Builder builder(TableMetadata table, boolean allowFiltering) {
      return new ClusteringColumnRestrictions.Builder(table, allowFiltering);
   }

   public static class Builder {
      private final TableMetadata table;
      private final boolean allowFiltering;
      private final RestrictionSet.Builder restrictions;

      private Builder(TableMetadata table, boolean allowFiltering) {
         this.restrictions = RestrictionSet.builder();
         this.table = table;
         this.allowFiltering = allowFiltering;
      }

      public ClusteringColumnRestrictions.Builder addRestriction(Restriction restriction) {
         SingleRestriction newRestriction = (SingleRestriction)restriction;
         boolean isEmpty = this.restrictions.isEmpty();
         if(!isEmpty && !this.allowFiltering) {
            SingleRestriction lastRestriction = this.restrictions.lastRestriction();
            ColumnMetadata lastRestrictionStart = lastRestriction.getFirstColumn();
            ColumnMetadata newRestrictionStart = newRestriction.getFirstColumn();
            this.restrictions.addRestriction(newRestriction);
            RequestValidations.checkFalse(lastRestriction.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(), "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)", newRestrictionStart.name, lastRestrictionStart.name);
            if(newRestrictionStart.position() < lastRestrictionStart.position() && newRestriction.isSlice()) {
               throw RequestValidations.invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)", new Object[]{this.restrictions.nextColumn(newRestrictionStart).name, newRestrictionStart.name});
            }
         } else {
            this.restrictions.addRestriction(newRestriction);
         }

         return this;
      }

      public ClusteringColumnRestrictions build() {
         return new ClusteringColumnRestrictions(this.table.comparator, this.restrictions.build());
      }
   }
}
