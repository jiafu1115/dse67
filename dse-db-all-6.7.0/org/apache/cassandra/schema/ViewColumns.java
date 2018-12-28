package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;

public class ViewColumns extends AbstractCollection<ViewColumnMetadata> {
   private final Set<ViewColumnMetadata> viewColumns;
   private final Map<ByteBuffer, ViewColumnMetadata> columnsByName;

   public ViewColumns(Set<ViewColumnMetadata> viewColumns) {
      this.viewColumns = viewColumns;
      this.columnsByName = new LinkedHashMap(viewColumns.size());
      Iterator var2 = viewColumns.iterator();

      while(var2.hasNext()) {
         ViewColumnMetadata viewColumn = (ViewColumnMetadata)var2.next();
         this.columnsByName.put(viewColumn.name().bytes, viewColumn);
         if(viewColumn.hasHiddenColumn()) {
            this.columnsByName.put(viewColumn.hiddenName().bytes, viewColumn);
         }
      }

   }

   public ViewColumnMetadata getByName(ColumnIdentifier columnName) {
      return (ViewColumnMetadata)this.columnsByName.get(columnName.bytes);
   }

   public Collection<ViewColumnMetadata> regularColumns() {
      return (Collection)this.viewColumns.stream().filter((c) -> {
         return c.isRegularColumn();
      }).collect(Collectors.toCollection(LinkedHashSet::<init>));
   }

   public Collection<ViewColumnMetadata> hiddenColumns() {
      return (Collection)this.viewColumns.stream().filter((c) -> {
         return c.hasHiddenColumn();
      }).collect(Collectors.toCollection(LinkedHashSet::<init>));
   }

   public Collection<ViewColumnMetadata> regularBaseColumnsInViewPrimaryKey() {
      return (Collection)this.viewColumns.stream().filter((c) -> {
         return c.isRegularBaseColumnInViewPrimaryKey();
      }).collect(Collectors.toCollection(LinkedHashSet::<init>));
   }

   public Collection<ViewColumnMetadata> requiredForLiveness() {
      return (Collection)this.viewColumns.stream().filter((c) -> {
         return c.isRequiredForLiveness();
      }).collect(Collectors.toCollection(LinkedHashSet::<init>));
   }

   public Iterator<ViewColumnMetadata> iterator() {
      return this.viewColumns.iterator();
   }

   public int size() {
      return this.viewColumns.size();
   }

   public static ViewColumns create(TableMetadata baseTableMetadata, Set<ColumnIdentifier> viewPrimaryKeys, Set<ColumnIdentifier> selectedColumns, WhereClause whereClause) {
      if(selectedColumns.isEmpty()) {
         selectedColumns = (Set)baseTableMetadata.columns().stream().map((s) -> {
            return s.name;
         }).collect(Collectors.toSet());
      }

      return create(baseTableMetadata, viewPrimaryKeys, selectedColumns, whereClause, (TableMetadata)null);
   }

   public static ViewColumns create(TableMetadata baseTableMetadata, TableMetadata viewTableMetadata, WhereClause whereClause) {
      Set<ColumnIdentifier> viewPrimaryKeys = (Set)viewTableMetadata.columns().stream().filter((s) -> {
         return s.isPrimaryKeyColumn();
      }).map((s) -> {
         return s.name;
      }).collect(Collectors.toSet());
      Set<ColumnIdentifier> selectedColumns = (Set)viewTableMetadata.columns().stream().filter((s) -> {
         return !s.isHidden();
      }).map((s) -> {
         return s.name;
      }).collect(Collectors.toSet());
      return create(baseTableMetadata, viewPrimaryKeys, selectedColumns, whereClause, viewTableMetadata);
   }

   private static ViewColumns create(TableMetadata baseTableMetadata, Set<ColumnIdentifier> viewPrimaryKeys, Set<ColumnIdentifier> selectedColumns, WhereClause whereClause, TableMetadata viewTableMetadata) {
      Set<ColumnIdentifier> basePrimaryKeys = (Set)baseTableMetadata.columns().stream().filter((c) -> {
         return c.isPrimaryKeyColumn();
      }).map((c) -> {
         return c.name;
      }).collect(Collectors.toSet());
      Set<ColumnIdentifier> filteredNonPrimaryKeys = (Set)computeFilteredColumns(baseTableMetadata, whereClause).stream().filter((s) -> {
         return !viewPrimaryKeys.contains(s);
      }).collect(Collectors.toSet());
      boolean hasRequiredColumnsForLiveness = basePrimaryKeys.size() != viewPrimaryKeys.size() || filteredNonPrimaryKeys.size() > 0;
      Set<ViewColumnMetadata> viewColumns = new LinkedHashSet(selectedColumns.size());
      Iterator var9 = baseTableMetadata.columns().iterator();

      while(true) {
         ViewColumnMetadata viewColumn;
         do {
            if(!var9.hasNext()) {
               return new ViewColumns(viewColumns);
            }

            ColumnMetadata baseColumn = (ColumnMetadata)var9.next();
            viewColumn = new ViewColumnMetadata(baseColumn.name, baseColumn.type, selectedColumns.contains(baseColumn.name), filteredNonPrimaryKeys.contains(baseColumn.name), basePrimaryKeys.contains(baseColumn.name), viewPrimaryKeys.contains(baseColumn.name), hasRequiredColumnsForLiveness);
            if(viewTableMetadata != null) {
               if(viewColumn.hasDataColumn()) {
                  viewColumn.setDataColumn(viewTableMetadata.getColumn(viewColumn.name()));
               }

               if(viewColumn.hasHiddenColumn()) {
                  ColumnMetadata hiddenColumn = viewTableMetadata.getColumn(viewColumn.hiddenName());
                  if(hiddenColumn != null && hiddenColumn.isHidden()) {
                     viewColumn.setHiddenColumn(hiddenColumn);
                  }
               }
            }
         } while(!viewColumn.hasDataColumn() && !viewColumn.hasHiddenColumn());

         viewColumns.add(viewColumn);
      }
   }

   public static Set<ColumnIdentifier> computeFilteredColumns(TableMetadata baseTableMetadata, WhereClause whereClause) {
      return (Set)whereClause.relations.stream().filter((s) -> {
         return s.operator() != Operator.IS_NOT;
      }).flatMap((r) -> {
         return r.toRestriction(baseTableMetadata, VariableSpecifications.empty()).getColumnDefs().stream();
      }).map((c) -> {
         return c.name;
      }).collect(Collectors.toSet());
   }
}
