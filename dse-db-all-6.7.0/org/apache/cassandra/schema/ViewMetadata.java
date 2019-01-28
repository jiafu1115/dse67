package org.apache.cassandra.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class ViewMetadata {
   public static final ViewMetadata.ViewVersion CURRENT_VERSION;
   public final String keyspace;
   public final String name;
   public final boolean includeAllColumns;
   public final TableMetadata baseTableMetadata;
   public final SelectStatement.RawStatement select;
   public final String whereClause;
   private final ViewColumns columns;
   protected final TableMetadata viewTableMetadata;
   private final Set<ColumnMetadata> regularBaseColumnsInViewPrimaryKey;
   private final ViewMetadata.ViewVersion viewVersion;

   public ViewMetadata(TableMetadata viewTableMetadata, TableMetadata baseTableMetadata, boolean includeAllColumns, List<Relation> whereClauseAsRelations, ViewMetadata.ViewVersion viewVersion) {
      this(viewTableMetadata, baseTableMetadata, includeAllColumns, buildWhereClause(whereClauseAsRelations), viewVersion);
   }

   public ViewMetadata(TableMetadata viewTableMetadata, TableMetadata baseTableMetadata, boolean includeAllColumns, String whereClause, ViewMetadata.ViewVersion viewVersion) {
      this(viewTableMetadata, baseTableMetadata, includeAllColumns, buildSelectStatement(viewTableMetadata, baseTableMetadata.name, includeAllColumns, whereClause), whereClause, viewVersion);
   }

   public ViewMetadata(TableMetadata viewTableMetadata, TableMetadata baseTableMetadata, boolean includeAllColumns, SelectStatement.RawStatement select, String whereClause, ViewMetadata.ViewVersion viewVersion) {
      this.keyspace = viewTableMetadata.keyspace;
      this.name = viewTableMetadata.name;
      this.includeAllColumns = includeAllColumns;
      this.select = select;
      this.whereClause = whereClause;
      this.baseTableMetadata = baseTableMetadata;
      this.viewTableMetadata = viewTableMetadata;
      this.columns = ViewColumns.create(baseTableMetadata, viewTableMetadata, select.whereClause);
      this.regularBaseColumnsInViewPrimaryKey = (Set)this.columns.regularBaseColumnsInViewPrimaryKey().stream().map((c) -> {
         return baseTableMetadata.getColumn(c.name());
      }).collect(Collectors.toCollection(LinkedHashSet::new));
      this.viewVersion = viewVersion;
      boolean isSchemaLegacy = isLegacyView(this.columns, viewTableMetadata);

      assert !viewVersion.isLegacy() == !isSchemaLegacy || viewVersion.isLegacy() && this.columns.hiddenColumns().isEmpty() : String.format("Expect view %s to be %s but got %s schema : %s", new Object[]{this.name, viewVersion.isLegacy()?"legacy":"new", isSchemaLegacy?"legacy":"new", viewTableMetadata.columns});

   }

   private static boolean isLegacyView(ViewColumns columns, TableMetadata viewTableMetadata) {
      return !columns.hiddenColumns().isEmpty() && viewTableMetadata.columns().stream().noneMatch((c) -> {
         return c.isHidden();
      }) || !columns.requiredForLiveness().isEmpty() && viewTableMetadata.columns().stream().noneMatch((c) -> {
         return c.isRequiredForLiveness;
      });
   }

   public ViewColumnMetadata getColumn(ColumnIdentifier columnName) {
      return this.columns.getByName(columnName);
   }

   public boolean hasSamePrimaryKeyColumnsAsBaseTable() {
      return this.regularBaseColumnsInViewPrimaryKey.isEmpty();
   }

   public Collection<ColumnMetadata> getRegularBaseColumnsInViewPrimaryKey() {
      return this.regularBaseColumnsInViewPrimaryKey;
   }

   public boolean isLegacyView() {
      return this.viewVersion.isLegacy();
   }

   public boolean shouldIncludeNewBaseColumns() {
      return this.includeAllColumns || !this.isLegacyView() && !this.viewTableMetadata.hasRequiredColumnsForLiveness();
   }

   public TableId viewId() {
      return this.viewTableMetadata.id;
   }

   public TableId baseTableId() {
      return this.baseTableMetadata.id;
   }

   public String baseTableName() {
      return this.baseTableMetadata.name;
   }

   public List<ColumnMetadata> clusteringColumns() {
      return this.viewTableMetadata.clusteringColumns();
   }

   public Iterable<ColumnMetadata> primaryKeyColumns() {
      return this.viewTableMetadata.primaryKeyColumns();
   }

   public List<ColumnMetadata> partitionKeyColumns() {
      return this.viewTableMetadata.partitionKeyColumns();
   }

   public TableParams getTableParameters() {
      return this.viewTableMetadata.params;
   }

   public PartitionUpdate createUpdate(DecoratedKey partitionKey) {
      return new PartitionUpdate(this.viewTableMetadata, partitionKey, this.viewTableMetadata.regularAndStaticColumns(), 4);
   }

   public ViewMetadata withRenamedPrimaryKey(TableMetadata newBaseTableMetadata, ColumnMetadata.Raw from, ColumnMetadata.Raw to) {
      ColumnIdentifier viewFrom = from.getIdentifier(this.viewTableMetadata);
      ColumnIdentifier viewTo = to.getIdentifier(this.viewTableMetadata);
      List<Relation> relations = this.select.whereClause.relations;
      ColumnMetadata.Raw fromRaw = ColumnMetadata.Raw.forQuoted(viewFrom.toString());
      ColumnMetadata.Raw toRaw = ColumnMetadata.Raw.forQuoted(viewTo.toString());
      List<Relation> newRelations = (List)relations.stream().map((r) -> {
         return r.renameIdentifier(fromRaw, toRaw);
      }).collect(Collectors.toList());
      return new ViewMetadata(this.viewTableMetadata.unbuild().renamePrimaryKeyColumn(viewFrom, viewTo).build(), newBaseTableMetadata, this.includeAllColumns, newRelations, this.viewVersion);
   }

   public ViewMetadata withAddedColumn(TableMetadata newBaseTableMetadata, ColumnMetadata baseColumn) {
      assert this.shouldIncludeNewBaseColumns() : String.format("View %s.%s does not require adding new columns", new Object[]{this.keyspace, this.name});

      boolean isHidden = !this.includeAllColumns;
      ColumnIdentifier columnName = isHidden?ViewColumnMetadata.hiddenViewColumnName(baseColumn.name):baseColumn.name;
      ColumnMetadata viewColumn = ColumnMetadata.regularColumn(this.keyspace, this.name, columnName, baseColumn.type, false, isHidden);
      return new ViewMetadata(this.viewTableMetadata.unbuild().addColumn(viewColumn).build(), newBaseTableMetadata, this.includeAllColumns, this.select, this.whereClause, this.viewVersion);
   }

   public ViewMetadata withDroppedColumn(TableMetadata newBaseTableMetadata, ColumnMetadata viewColumn, long droppedTime) {
      TableMetadata.Builder builder = this.viewTableMetadata.unbuild().removeRegularOrStaticColumn(viewColumn.name).recordColumnDrop(viewColumn, droppedTime);
      return new ViewMetadata(builder.build(), newBaseTableMetadata, this.includeAllColumns, this.select, this.whereClause, this.viewVersion);
   }

   public ViewMetadata withUpdatedParameters(TableParams newParams) {
      return new ViewMetadata(this.viewTableMetadata.unbuild().params(newParams).build(), this.baseTableMetadata, this.includeAllColumns, this.select, this.whereClause, this.viewVersion);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ViewMetadata)) {
         return false;
      } else {
         ViewMetadata other = (ViewMetadata)o;
         return Objects.equals(this.keyspace, other.keyspace) && Objects.equals(this.name, other.name) && Objects.equals(this.baseTableMetadata, other.baseTableMetadata) && Objects.equals(Boolean.valueOf(this.includeAllColumns), Boolean.valueOf(other.includeAllColumns)) && Objects.equals(this.whereClause, other.whereClause) && Objects.equals(this.viewTableMetadata, other.viewTableMetadata);
      }
   }

   public int hashCode() {
      return (new HashCodeBuilder(29, 1597)).append(this.keyspace).append(this.name).append(this.baseTableMetadata).append(this.includeAllColumns).append(this.whereClause).append(this.viewTableMetadata).toHashCode();
   }

   public String toString() {
      return (new ToStringBuilder(this)).append("keyspace", this.keyspace).append("name", this.name).append("baseTableId", this.baseTableId()).append("baseTableName", this.baseTableName()).append("includeAllColumns", this.includeAllColumns).append("whereClause", this.whereClause).append("viewTableMetadata", this.viewTableMetadata).toString();
   }

   private static SelectStatement.RawStatement buildSelectStatement(TableMetadata viewTableMetadata, String baseTableName, boolean includeAllColumns, String whereClause) {
      StringBuilder rawSelect = new StringBuilder("SELECT ");
      if(includeAllColumns) {
         rawSelect.append("*");
      } else {
         rawSelect.append((String)viewTableMetadata.columns().stream().filter((c) -> {
            return !c.isHidden();
         }).map((id) -> {
            return id.name.toCQLString();
         }).collect(Collectors.joining(", ")));
      }

      rawSelect.append(" FROM \"").append(baseTableName).append("\" WHERE ").append(whereClause).append(" ALLOW FILTERING");
      return (SelectStatement.RawStatement)QueryProcessor.parseStatement(rawSelect.toString());
   }

   private static String buildWhereClause(List<Relation> whereClauseAsRelations) {
      List<String> expressions = new ArrayList(whereClauseAsRelations.size());

      StringBuilder sb;
      for(Iterator var2 = whereClauseAsRelations.iterator(); var2.hasNext(); expressions.add(sb.toString())) {
         Relation rel = (Relation)var2.next();
         sb = new StringBuilder();
         if(rel.isMultiColumn()) {
            sb.append((String)((MultiColumnRelation)rel).getEntities().stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")")));
         } else {
            sb.append(((SingleColumnRelation)rel).getEntity());
         }

         sb.append(" ").append(rel.operator()).append(" ");
         if(rel.isIN()) {
            sb.append((String)rel.getInValues().stream().map(Term.Raw::getText).collect(Collectors.joining(", ", "(", ")")));
         } else {
            sb.append(rel.getValue().getText());
         }
      }

      return (String)expressions.stream().collect(Collectors.joining(" AND "));
   }

   public boolean purgeRowsWithEmptyPrimaryKey() {
      return this.viewVersion.isLegacy() && !this.hasSamePrimaryKeyColumnsAsBaseTable();
   }

   public boolean hasUnselectedColumns() {
      return this.columns.stream().anyMatch((c) -> {
         return !c.isSelected();
      });
   }

   public boolean hasFilteredNonPrimaryKeyColumns() {
      return this.columns.stream().anyMatch((c) -> {
         return !c.isFilteredNonPrimaryKey();
      });
   }

   public ViewMetadata.ViewVersion getVersion() {
      return this.viewVersion;
   }

   static {
      CURRENT_VERSION = ViewMetadata.ViewVersion.V1;
   }

   public static enum ViewVersion {
      V0,
      V1;

      private ViewVersion() {
      }

      boolean isLegacy() {
         return this == V0;
      }
   }
}
