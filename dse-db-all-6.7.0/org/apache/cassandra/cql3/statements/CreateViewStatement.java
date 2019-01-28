package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.reactivex.Maybe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.view.ViewFeature;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewColumns;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public class CreateViewStatement extends SchemaAlteringStatement implements TableStatement {
   private final CFName baseName;
   private final List<RawSelector> selectClause;
   private final WhereClause whereClause;
   private final List<ColumnMetadata.Raw> partitionKeys;
   private final List<ColumnMetadata.Raw> clusteringKeys;
   public final CFProperties properties = new CFProperties();
   private final boolean ifNotExists;

   public CreateViewStatement(CFName viewName, CFName baseName, List<RawSelector> selectClause, WhereClause whereClause, List<ColumnMetadata.Raw> partitionKeys, List<ColumnMetadata.Raw> clusteringKeys, boolean ifNotExists) {
      super(viewName);
      this.baseName = baseName;
      this.selectClause = selectClause;
      this.whereClause = whereClause;
      this.partitionKeys = partitionKeys;
      this.clusteringKeys = clusteringKeys;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_VIEW;
   }

   public void checkAccess(QueryState state) {
      if(!this.baseName.hasKeyspace()) {
         this.baseName.setKeyspace(this.keyspace(), true);
      }

      state.checkTablePermission(this.keyspace(), this.baseName.getColumnFamily(), CorePermission.ALTER);
   }

   public void validate(QueryState state) throws RequestValidationException {
   }

   private void add(TableMetadata baseCfm, Iterable<ColumnIdentifier> columns, CreateViewStatement.AddColumn adder) {
      ColumnIdentifier column;
      Object type;
      for(Iterator var4 = columns.iterator(); var4.hasNext(); adder.add(column, (AbstractType)type)) {
         column = (ColumnIdentifier)var4.next();
         type = baseCfm.getColumn(column).type;
         if(this.properties.definedOrdering.containsKey(column)) {
            boolean desc = ((Boolean)this.properties.definedOrdering.get(column)).booleanValue();
            if(!desc && ((AbstractType)type).isReversed()) {
               type = ((ReversedType)type).baseType;
            } else if(desc && !((AbstractType)type).isReversed()) {
               type = ReversedType.getInstance((AbstractType)type);
            }
         }
      }

   }

   public Maybe<Event.SchemaChange> announceMigration(final QueryState queryState, final boolean isLocalOnly) throws RequestValidationException {
      this.properties.validate();
      if (this.properties.useCompactStorage) {
         return this.error("Cannot use 'COMPACT STORAGE' when defining a materialized view");
      }
      if (!this.baseName.getKeyspace().equals(this.keyspace())) {
         return this.error("Cannot create a materialized view on a table in a separate keyspace");
      }
      final TableMetadata baseTableMetadata = Schema.instance.validateTable(this.baseName.getKeyspace(), this.baseName.getColumnFamily());
      if (baseTableMetadata.isCounter()) {
         return this.error("Materialized views are not supported on counter tables");
      }
      if (baseTableMetadata.isView()) {
         return this.error("Materialized views cannot be created against other materialized views");
      }
      if (baseTableMetadata.params.gcGraceSeconds == 0) {
         return this.error(String.format("Cannot create materialized view '%s' for base table '%s' with gc_grace_seconds of 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.", this.cfName.getColumnFamily(), this.baseName.getColumnFamily()));
      }
      final Set<ColumnIdentifier> selectedColumns = Sets.newHashSetWithExpectedSize(this.selectClause.size());
      for (final RawSelector selector : this.selectClause) {
         final Selectable.Raw selectable = selector.selectable;
         if (selectable instanceof Selectable.WithFieldSelection.Raw) {
            return this.error("Cannot select out a part of type when defining a materialized view");
         }
         if (selectable instanceof Selectable.WithFunction.Raw) {
            return this.error("Cannot use function when defining a materialized view");
         }
         if (selectable instanceof Selectable.WritetimeOrTTL.Raw) {
            return this.error("Cannot use function when defining a materialized view");
         }
         if (selectable instanceof Selectable.WithElementSelection.Raw) {
            return this.error("Cannot use collection element selection when defining a materialized view");
         }
         if (selectable instanceof Selectable.WithSliceSelection.Raw) {
            return this.error("Cannot use collection slice selection when defining a materialized view");
         }
         if (selector.alias != null) {
            return this.error("Cannot use alias when defining a materialized view");
         }
         final Selectable s = selectable.prepare(baseTableMetadata);
         if (s instanceof Term.Raw) {
            return this.error("Cannot use terms in selection when defining a materialized view");
         }
         final ColumnMetadata cdef = (ColumnMetadata)s;
         selectedColumns.add(cdef.name);
      }
      final Set<ColumnMetadata.Raw> targetPrimaryKeys = SetsFactory.newSet();
      for (final ColumnMetadata.Raw identifier : Iterables.concat(this.partitionKeys, this.clusteringKeys)) {
         if (!targetPrimaryKeys.add(identifier)) {
            return this.error("Duplicate entry found in PRIMARY KEY: " + identifier);
         }
         final ColumnMetadata cdef2 = identifier.prepare(baseTableMetadata);
         if (cdef2.type.isMultiCell()) {
            return this.error(String.format("Cannot use MultiCell column '%s' in PRIMARY KEY of materialized view", identifier));
         }
         if (cdef2.isStatic()) {
            return this.error(String.format("Cannot use Static column '%s' in PRIMARY KEY of materialized view", identifier));
         }
         if (cdef2.type instanceof DurationType) {
            return this.error(String.format("Cannot use Duration column '%s' in PRIMARY KEY of materialized view", identifier));
         }
      }
      final Map<ColumnMetadata.Raw, Boolean> orderings = Collections.emptyMap();
      final List<Selectable.Raw> groups = UnmodifiableArrayList.emptyList();
      final SelectStatement.Parameters parameters = new SelectStatement.Parameters(orderings, groups, false, true, false);
      final SelectStatement.RawStatement rawSelect = new SelectStatement.RawStatement(this.baseName, parameters, this.selectClause, this.whereClause, null, null);
      final ClientState state = ClientState.forInternalCalls();
      state.setKeyspace(this.keyspace());
      rawSelect.prepareKeyspace(state);
      rawSelect.setBoundVariables(this.getBoundVariables());
      final Prepared prepared = rawSelect.prepare(true);
      final SelectStatement select = (SelectStatement)prepared.statement;
      final StatementRestrictions restrictions = select.getRestrictions();
      if (!prepared.boundNames.isEmpty()) {
         return this.error("Cannot use query parameters in CREATE MATERIALIZED VIEW statements");
      }
      for (final ColumnMetadata restricted : restrictions.nonPKRestrictedColumns(false)) {
         if (restricted.type.isMultiCell()) {
            throw new InvalidRequestException(String.format("Non-primary-key multi-cell columns cannot be restricted in the SELECT statement used for materialized view '%s' creation (got restriction on: %s)", this.columnFamily(), restricted.name.toString()));
         }
      }
      final Set<ColumnIdentifier> basePrimaryKeyCols = SetsFactory.newSet();
      for (final ColumnMetadata definition : Iterables.concat(baseTableMetadata.partitionKeyColumns(), baseTableMetadata.clusteringColumns())) {
         basePrimaryKeyCols.add(definition.name);
      }
      final List<ColumnIdentifier> targetClusteringColumns = new ArrayList<ColumnIdentifier>();
      final List<ColumnIdentifier> targetPartitionKeys = new ArrayList<ColumnIdentifier>();
      for (final ColumnMetadata.Raw raw : this.partitionKeys) {
         addColumnIdentifier(baseTableMetadata, raw, targetPartitionKeys, restrictions);
      }
      for (final ColumnMetadata.Raw raw : this.clusteringKeys) {
         addColumnIdentifier(baseTableMetadata, raw, targetClusteringColumns, restrictions);
      }
      final List<ColumnIdentifier> missingBasePrimaryKeys = new ArrayList<ColumnIdentifier>();
      for (final ColumnMetadata def : baseTableMetadata.columns()) {
         final ColumnIdentifier identifier2 = def.name;
         final boolean includeDef = selectedColumns.isEmpty() || selectedColumns.contains(identifier2);
         if (includeDef && def.isStatic()) {
            return this.error(String.format("Unable to include static column '%s' which would be included by Materialized View SELECT * statement", identifier2));
         }
         final boolean defInTargetPrimaryKey = targetClusteringColumns.contains(identifier2) || targetPartitionKeys.contains(identifier2);
         if (!def.isPrimaryKeyColumn() || defInTargetPrimaryKey) {
            continue;
         }
         missingBasePrimaryKeys.add(identifier2);
      }
      if (!missingBasePrimaryKeys.isEmpty()) {
         return this.error(String.format("Cannot create Materialized View %s without primary key columns from base %s (%s)", this.columnFamily(), this.baseName.getColumnFamily(), Joiner.on(",").join((Iterable)missingBasePrimaryKeys)));
      }
      if (targetPartitionKeys.isEmpty()) {
         return this.error("Must select at least a column for a Materialized View");
      }
      if (targetClusteringColumns.isEmpty()) {
         return this.error("No columns are defined for Materialized View other than primary key");
      }
      final TableParams params = this.properties.properties.asNewTableParams();
      if (params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
         DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
      }
      if (params.defaultTimeToLive > 0) {
         throw new InvalidRequestException("Cannot set default_time_to_live for a materialized view. Data in a materialized view always expire at the same time than the corresponding data in the parent table.");
      }
      final LinkedHashSet<ColumnIdentifier> viewPrimaryKeys = Stream.concat(targetPartitionKeys.stream(), targetClusteringColumns.stream()).collect(Collectors.toCollection(LinkedHashSet::new));
      final ViewColumns viewColumns = ViewColumns.create(baseTableMetadata, viewPrimaryKeys, selectedColumns, this.whereClause);
      if (viewColumns.regularBaseColumnsInViewPrimaryKey().size() > 1) {
         final List<ColumnIdentifier> basePartitionKeys = baseTableMetadata.partitionKeyColumns().stream().map(c -> c.name).collect(Collectors.toList());
         if (!basePartitionKeys.equals(targetPartitionKeys) && !ViewFeature.allowMultipleRegularBaseColumnsInViewPrimaryKey()) {
            throw new InvalidRequestException(String.format("Cannot include more than one non-base primary key column in materialized view primary key when partition key is not the same. View partition key is %s and base partition key is %s.", targetPartitionKeys, basePartitionKeys));
         }
      }
      final TableMetadata.Builder builder = TableMetadata.builder(this.keyspace(), this.columnFamily(), this.properties.properties.getId()).kind(TableMetadata.Kind.VIEW).params(params);
      this.add(baseTableMetadata, targetPartitionKeys, builder::addPartitionKeyColumn);
      this.add(baseTableMetadata, targetClusteringColumns, builder::addClusteringColumn);
      final boolean isNewView = ViewFeature.useNewMaterializedView();
      viewColumns.regularColumns().stream().forEach(c -> builder.addRegularColumn(c.name(), baseTableMetadata.getColumn(c.name()).type, isNewView && c.isRequiredForLiveness()));
      if (isNewView) {
         viewColumns.hiddenColumns().stream().forEach(c -> builder.addRegularColumn(c.hiddenName(), baseTableMetadata.getColumn(c.name()).type, c.isRequiredForLiveness(), true));
      }
      final TableMetadata viewTableMetadata = builder.build();
      final ViewMetadata definition2 = new ViewMetadata(viewTableMetadata, baseTableMetadata, selectedColumns.isEmpty(), this.whereClause.relations, isNewView ? ViewMetadata.ViewVersion.V1 : ViewMetadata.ViewVersion.V0);
      if (viewTableMetadata.hasRequiredColumnsForLiveness() || viewTableMetadata.columns().stream().anyMatch(c -> c.isHidden())) {
         ViewFeature.validateViewVersion();
      }
      return MigrationManager.announceNewView(definition2, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily()))).onErrorResumeNext(e -> {
         if (e instanceof AlreadyExistsException && this.ifNotExists) {
            return Maybe.empty();
         }
         return Maybe.error(e);
      });
   }


   private static void addColumnIdentifier(TableMetadata cfm, ColumnMetadata.Raw raw, List<ColumnIdentifier> columns, StatementRestrictions restrictions) {
      ColumnMetadata def = raw.prepare(cfm);
      boolean isSinglePartitionKey = def.isPartitionKey() && cfm.partitionKeyColumns().size() == 1;
      if(!isSinglePartitionKey && !restrictions.isRestricted(def)) {
         throw new InvalidRequestException(String.format("Primary key column '%s' is required to be filtered by 'IS NOT NULL'", new Object[]{def.name}));
      } else {
         columns.add(def.name);
      }
   }

   private interface AddColumn {
      void add(ColumnIdentifier var1, AbstractType<?> var2);
   }
}
