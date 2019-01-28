package org.apache.cassandra.cql3.restrictions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public abstract class RestrictionSet implements Restrictions {
   private static final Comparator<ColumnMetadata> COLUMN_DEFINITION_COMPARATOR = Comparator.comparingInt(ColumnMetadata::position).thenComparing((column) -> {
      return column.name.bytes;
   });

   public RestrictionSet() {
   }

   public abstract boolean hasRestrictionFor(ColumnMetadata.Kind var1);

   public abstract SingleRestriction lastRestriction();

   public abstract boolean hasMultipleContains();

   public abstract List<SingleRestriction> restrictions();

   public abstract boolean hasMultiColumnSlice();

   public static RestrictionSet.Builder builder() {
      return new RestrictionSet.Builder();
   }

   public static final class Builder {
      private final Map<ColumnMetadata, SingleRestriction> newRestrictions;
      private boolean multiColumn;
      private ColumnMetadata lastRestrictionColumn;
      private SingleRestriction lastRestriction;

      private Builder() {
         this.newRestrictions = new HashMap();
         this.multiColumn = false;
      }

      public void addRestriction(SingleRestriction restriction) {
         List<ColumnMetadata> columnDefs = restriction.getColumnDefs();
         Set<SingleRestriction> existingRestrictions = getRestrictions(this.newRestrictions, columnDefs);
         if(existingRestrictions.isEmpty()) {
            this.addRestrictionForColumns(columnDefs, restriction);
         } else {
            Iterator var4 = existingRestrictions.iterator();

            while(var4.hasNext()) {
               SingleRestriction existing = (SingleRestriction)var4.next();
               SingleRestriction newRestriction = existing.mergeWith(restriction);
               this.addRestrictionForColumns(columnDefs, newRestriction);
            }
         }

      }

      private void addRestrictionForColumns(List<ColumnMetadata> columnDefs, SingleRestriction restriction) {
         for(int i = 0; i < columnDefs.size(); ++i) {
            ColumnMetadata column = (ColumnMetadata)columnDefs.get(i);
            if(this.lastRestrictionColumn == null || RestrictionSet.COLUMN_DEFINITION_COMPARATOR.compare(this.lastRestrictionColumn, column) < 0) {
               this.lastRestrictionColumn = column;
               this.lastRestriction = restriction;
            }

            this.newRestrictions.put(column, restriction);
         }

         this.multiColumn |= restriction.isMultiColumn();
      }

      private static Set<SingleRestriction> getRestrictions(Map<ColumnMetadata, SingleRestriction> restrictions, List<ColumnMetadata> columnDefs) {
         Set<SingleRestriction> set = SetsFactory.newSet();

         for(int i = 0; i < columnDefs.size(); ++i) {
            SingleRestriction existing = (SingleRestriction)restrictions.get(columnDefs.get(i));
            if(existing != null) {
               set.add(existing);
            }
         }

         return set;
      }

      public RestrictionSet build() {
         return (RestrictionSet)(this.isEmpty()?RestrictionSet.EmptyRestrictionSet.INSTANCE:new RestrictionSet.DefaultRestrictionSet(this.newRestrictions, this.multiColumn));
      }

      public boolean isEmpty() {
         return this.newRestrictions.isEmpty();
      }

      public SingleRestriction lastRestriction() {
         return this.lastRestriction;
      }

      public ColumnMetadata nextColumn(ColumnMetadata columnDef) {
         NavigableSet<ColumnMetadata> columns = new TreeSet(RestrictionSet.COLUMN_DEFINITION_COMPARATOR);
         columns.addAll(this.newRestrictions.keySet());
         return (ColumnMetadata)columns.tailSet(columnDef, false).first();
      }
   }

   private static final class DefaultRestrictionSet extends RestrictionSet {
      private final List<ColumnMetadata> restrictionsKeys;
      private final List<SingleRestriction> restrictionsValues;
      private final Map<ColumnMetadata, SingleRestriction> restrictionsHashMap;
      private final int hasBitmap;
      private final int restrictionForKindBitmap;
      private static final int maskHasContains = 1;
      private static final int maskHasSlice = 2;
      private static final int maskHasIN = 4;
      private static final int maskHasOnlyEqualityRestrictions = 8;
      private static final int maskHasMultiColumnSlice = 16;
      private static final int maskHasMultipleContains = 32;

      private DefaultRestrictionSet(Map<ColumnMetadata, SingleRestriction> restrictions, boolean hasMultiColumnRestrictions) {
         this.restrictionsKeys = new ArrayList(restrictions.keySet());
         this.restrictionsKeys.sort(RestrictionSet.COLUMN_DEFINITION_COMPARATOR);
         UnmodifiableArrayList.Builder<SingleRestriction> sortedRestrictions = UnmodifiableArrayList.builder(restrictions.size());
         int numberOfContains = 0;
         int restrictionForBitmap = 0;
         int bitmap = 8;
         SingleRestriction previous = null;

         for(int i = 0; i < this.restrictionsKeys.size(); ++i) {
            ColumnMetadata col = (ColumnMetadata)this.restrictionsKeys.get(i);
            SingleRestriction singleRestriction = (SingleRestriction)restrictions.get(col);
            if(singleRestriction.isContains()) {
               bitmap |= 1;
               SingleColumnRestriction.ContainsRestriction contains = (SingleColumnRestriction.ContainsRestriction)singleRestriction;
               numberOfContains += contains.numberOfValues() + contains.numberOfKeys() + contains.numberOfEntries();
            }

            if(hasMultiColumnRestrictions) {
               if(singleRestriction.equals(previous)) {
                  continue;
               }

               previous = singleRestriction;
            }

            restrictionForBitmap |= 1 << col.kind.ordinal();
            sortedRestrictions.add(singleRestriction);
            if(singleRestriction.isSlice()) {
               bitmap |= 2;
               if(singleRestriction.isMultiColumn()) {
                  bitmap |= 16;
               }
            }

            if(singleRestriction.isIN()) {
               bitmap |= 4;
            } else if(!singleRestriction.isEQ()) {
               bitmap &= -9;
            }
         }

         this.hasBitmap = bitmap | (numberOfContains > 1?32:0);
         this.restrictionForKindBitmap = restrictionForBitmap;
         this.restrictionsValues = sortedRestrictions.build();
         this.restrictionsHashMap = restrictions;
      }

      public void addRowFilterTo(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) throws InvalidRequestException {
         Iterator var4 = this.restrictionsHashMap.values().iterator();

         while(var4.hasNext()) {
            SingleRestriction restriction = (SingleRestriction)var4.next();
            restriction.addRowFilterTo(filter, indexRegistry, options);
         }

      }

      public List<ColumnMetadata> getColumnDefs() {
         return this.restrictionsKeys;
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
         for(int i = 0; i < this.restrictionsValues.size(); ++i) {
            ((SingleRestriction)this.restrictionsValues.get(i)).addFunctionsTo(functions);
         }

      }

      public void forEachFunction(Consumer<org.apache.cassandra.cql3.functions.Function> consumer) {
         for(int i = 0; i < this.restrictionsValues.size(); ++i) {
            ((SingleRestriction)this.restrictionsValues.get(i)).forEachFunction(consumer);
         }

      }

      public boolean isEmpty() {
         return false;
      }

      public int size() {
         return this.restrictionsKeys.size();
      }

      public boolean hasRestrictionFor(ColumnMetadata.Kind kind) {
         return 0 != (this.restrictionForKindBitmap & 1 << kind.ordinal());
      }

      public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
         Restriction existing = (Restriction)this.restrictionsHashMap.get(columnDef);
         return existing == null?Collections.emptySet():Collections.singleton(existing);
      }

      public boolean hasSupportingIndex(IndexRegistry indexRegistry) {
         Iterator var2 = this.restrictionsHashMap.values().iterator();

         SingleRestriction restriction;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            restriction = (SingleRestriction)var2.next();
         } while(!restriction.hasSupportingIndex(indexRegistry));

         return true;
      }

      public ColumnMetadata getFirstColumn() {
         return (ColumnMetadata)this.restrictionsKeys.get(0);
      }

      public ColumnMetadata getLastColumn() {
         return (ColumnMetadata)this.restrictionsKeys.get(this.restrictionsKeys.size() - 1);
      }

      public SingleRestriction lastRestriction() {
         return (SingleRestriction)this.restrictionsValues.get(this.restrictionsValues.size() - 1);
      }

      public boolean hasMultipleContains() {
         return 0 != (this.hasBitmap & 32);
      }

      public List<SingleRestriction> restrictions() {
         return this.restrictionsValues;
      }

      public boolean hasIN() {
         return 0 != (this.hasBitmap & 4);
      }

      public boolean hasContains() {
         return 0 != (this.hasBitmap & 1);
      }

      public boolean hasSlice() {
         return 0 != (this.hasBitmap & 2);
      }

      public boolean hasMultiColumnSlice() {
         return 0 != (this.hasBitmap & 16);
      }

      public boolean hasOnlyEqualityRestrictions() {
         return 0 != (this.hasBitmap & 8);
      }
   }

   private static final class EmptyRestrictionSet extends RestrictionSet {
      private static final RestrictionSet.EmptyRestrictionSet INSTANCE = new RestrictionSet.EmptyRestrictionSet();

      private EmptyRestrictionSet() {
      }

      public void addRowFilterTo(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) throws InvalidRequestException {
      }

      public List<ColumnMetadata> getColumnDefs() {
         return UnmodifiableArrayList.emptyList();
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
      }

      public void forEachFunction(Consumer<org.apache.cassandra.cql3.functions.Function> consumer) {
      }

      public boolean isEmpty() {
         return true;
      }

      public int size() {
         return 0;
      }

      public boolean hasRestrictionFor(ColumnMetadata.Kind kind) {
         return false;
      }

      public Set<Restriction> getRestrictions(ColumnMetadata columnDef) {
         return Collections.emptySet();
      }

      public boolean hasSupportingIndex(IndexRegistry indexRegistry) {
         return false;
      }

      public ColumnMetadata getFirstColumn() {
         return null;
      }

      public ColumnMetadata getLastColumn() {
         return null;
      }

      public SingleRestriction lastRestriction() {
         return null;
      }

      public boolean hasMultipleContains() {
         return false;
      }

      public List<SingleRestriction> restrictions() {
         return UnmodifiableArrayList.emptyList();
      }

      public boolean hasMultiColumnSlice() {
         return false;
      }
   }
}
