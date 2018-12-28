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

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      this.properties.validate();
      if(this.properties.useCompactStorage) {
         return this.error("Cannot use 'COMPACT STORAGE' when defining a materialized view");
      } else if(!this.baseName.getKeyspace().equals(this.keyspace())) {
         return this.error("Cannot create a materialized view on a table in a separate keyspace");
      } else {
         TableMetadata baseTableMetadata = Schema.instance.validateTable(this.baseName.getKeyspace(), this.baseName.getColumnFamily());
         if(baseTableMetadata.isCounter()) {
            return this.error("Materialized views are not supported on counter tables");
         } else if(baseTableMetadata.isView()) {
            return this.error("Materialized views cannot be created against other materialized views");
         } else if(baseTableMetadata.params.gcGraceSeconds == 0) {
            return this.error(String.format("Cannot create materialized view '%s' for base table '%s' with gc_grace_seconds of 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.", new Object[]{this.cfName.getColumnFamily(), this.baseName.getColumnFamily()}));
         } else {
            Set<ColumnIdentifier> selectedColumns = Sets.newHashSetWithExpectedSize(this.selectClause.size());
            Iterator var5 = this.selectClause.iterator();

            while(var5.hasNext()) {
               RawSelector selector = (RawSelector)var5.next();
               Selectable.Raw selectable = selector.selectable;
               if(selectable instanceof Selectable.WithFieldSelection.Raw) {
                  return this.error("Cannot select out a part of type when defining a materialized view");
               }

               if(selectable instanceof Selectable.WithFunction.Raw) {
                  return this.error("Cannot use function when defining a materialized view");
               }

               if(selectable instanceof Selectable.WritetimeOrTTL.Raw) {
                  return this.error("Cannot use function when defining a materialized view");
               }

               if(selectable instanceof Selectable.WithElementSelection.Raw) {
                  return this.error("Cannot use collection element selection when defining a materialized view");
               }

               if(selectable instanceof Selectable.WithSliceSelection.Raw) {
                  return this.error("Cannot use collection slice selection when defining a materialized view");
               }

               if(selector.alias != null) {
                  return this.error("Cannot use alias when defining a materialized view");
               }

               Selectable s = selectable.prepare(baseTableMetadata);
               if(s instanceof Term.Raw) {
                  return this.error("Cannot use terms in selection when defining a materialized view");
               }

               ColumnMetadata cdef = (ColumnMetadata)s;
               selectedColumns.add(cdef.name);
            }

            Set<ColumnMetadata.Raw> targetPrimaryKeys = SetsFactory.newSet();
            Iterator var26 = Iterables.concat(this.partitionKeys, this.clusteringKeys).iterator();

            while(var26.hasNext()) {
               ColumnMetadata.Raw identifier = (ColumnMetadata.Raw)var26.next();
               if(!targetPrimaryKeys.add(identifier)) {
                  return this.error("Duplicate entry found in PRIMARY KEY: " + identifier);
               }

               ColumnMetadata cdef = identifier.prepare(baseTableMetadata);
               if(cdef.type.isMultiCell()) {
                  return this.error(String.format("Cannot use MultiCell column '%s' in PRIMARY KEY of materialized view", new Object[]{identifier}));
               }

               if(cdef.isStatic()) {
                  return this.error(String.format("Cannot use Static column '%s' in PRIMARY KEY of materialized view", new Object[]{identifier}));
               }

               if(cdef.type instanceof DurationType) {
                  return this.error(String.format("Cannot use Duration column '%s' in PRIMARY KEY of materialized view", new Object[]{identifier}));
               }
            }

            Map<ColumnMetadata.Raw, Boolean> orderings = Collections.emptyMap();
            List<Selectable.Raw> groups = UnmodifiableArrayList.emptyList();
            SelectStatement.Parameters parameters = new SelectStatement.Parameters(orderings, groups, false, true, false);
            SelectStatement.RawStatement rawSelect = new SelectStatement.RawStatement(this.baseName, parameters, this.selectClause, this.whereClause, (Term.Raw)null, (Term.Raw)null);
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(this.keyspace());
            rawSelect.prepareKeyspace(state);
            rawSelect.setBoundVariables(this.getBoundVariables());
            ParsedStatement.Prepared prepared = rawSelect.prepare(true);
            SelectStatement select = (SelectStatement)prepared.statement;
            StatementRestrictions restrictions = select.getRestrictions();
            if(!prepared.boundNames.isEmpty()) {
               return this.error("Cannot use query parameters in CREATE MATERIALIZED VIEW statements");
            } else {
               Iterator var14 = restrictions.nonPKRestrictedColumns(false).iterator();

               while(var14.hasNext()) {
                  ColumnMetadata restricted = (ColumnMetadata)var14.next();
                  if(restricted.type.isMultiCell()) {
                     throw new InvalidRequestException(String.format("Non-primary-key multi-cell columns cannot be restricted in the SELECT statement used for materialized view '%s' creation (got restriction on: %s)", new Object[]{this.columnFamily(), restricted.name.toString()}));
                  }
               }

               Set<ColumnIdentifier> basePrimaryKeyCols = SetsFactory.newSet();
               Iterator var34 = Iterables.concat(baseTableMetadata.partitionKeyColumns(), baseTableMetadata.clusteringColumns()).iterator();

               while(var34.hasNext()) {
                  ColumnMetadata definition = (ColumnMetadata)var34.next();
                  basePrimaryKeyCols.add(definition.name);
               }

               List<ColumnIdentifier> targetClusteringColumns = new ArrayList();
               List<ColumnIdentifier> targetPartitionKeys = new ArrayList();
               Iterator var17 = this.partitionKeys.iterator();

               ColumnMetadata.Raw raw;
               while(var17.hasNext()) {
                  raw = (ColumnMetadata.Raw)var17.next();
                  addColumnIdentifier(baseTableMetadata, raw, targetPartitionKeys, restrictions);
               }

               var17 = this.clusteringKeys.iterator();

               while(var17.hasNext()) {
                  raw = (ColumnMetadata.Raw)var17.next();
                  addColumnIdentifier(baseTableMetadata, raw, targetClusteringColumns, restrictions);
               }

               List<ColumnIdentifier> missingBasePrimaryKeys = new ArrayList();
               Iterator var38 = baseTableMetadata.columns().iterator();

               boolean isNewView;
               while(var38.hasNext()) {
                  ColumnMetadata def = (ColumnMetadata)var38.next();
                  ColumnIdentifier identifier = def.name;
                  boolean includeDef = selectedColumns.isEmpty() || selectedColumns.contains(identifier);
                  if(includeDef && def.isStatic()) {
                     return this.error(String.format("Unable to include static column '%s' which would be included by Materialized View SELECT * statement", new Object[]{identifier}));
                  }

                  isNewView = targetClusteringColumns.contains(identifier) || targetPartitionKeys.contains(identifier);
                  if(def.isPrimaryKeyColumn() && !isNewView) {
                     missingBasePrimaryKeys.add(identifier);
                  }
               }

               if(!missingBasePrimaryKeys.isEmpty()) {
                  return this.error(String.format("Cannot create Materialized View %s without primary key columns from base %s (%s)", new Object[]{this.columnFamily(), this.baseName.getColumnFamily(), Joiner.on(",").join(missingBasePrimaryKeys)}));
               } else if(targetPartitionKeys.isEmpty()) {
                  return this.error("Must select at least a column for a Materialized View");
               } else if(targetClusteringColumns.isEmpty()) {
                  return this.error("No columns are defined for Materialized View other than primary key");
               } else {
                  TableParams params = this.properties.properties.asNewTableParams();
                  if(params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
                     DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
                  }

                  if(params.defaultTimeToLive > 0) {
                     throw new InvalidRequestException("Cannot set default_time_to_live for a materialized view. Data in a materialized view always expire at the same time than the corresponding data in the parent table.");
                  } else {
                     LinkedHashSet<ColumnIdentifier> viewPrimaryKeys = (LinkedHashSet)Stream.concat(targetPartitionKeys.stream(), targetClusteringColumns.stream()).collect(Collectors.toCollection(LinkedHashSet::<init>));
                     ViewColumns viewColumns = ViewColumns.create(baseTableMetadata, viewPrimaryKeys, selectedColumns, this.whereClause);
                     if(viewColumns.regularBaseColumnsInViewPrimaryKey().size() > 1) {
                        List<ColumnIdentifier> basePartitionKeys = (List)baseTableMetadata.partitionKeyColumns().stream().map((c) -> {
                           return c.name;
                        }).collect(Collectors.toList());
                        if(!basePartitionKeys.equals(targetPartitionKeys) && !ViewFeature.allowMultipleRegularBaseColumnsInViewPrimaryKey()) {
                           throw new InvalidRequestException(String.format("Cannot include more than one non-base primary key column in materialized view primary key when partition key is not the same. View partition key is %s and base partition key is %s.", new Object[]{targetPartitionKeys, basePartitionKeys}));
                        }
                     }

                     TableMetadata.Builder builder = TableMetadata.builder(this.keyspace(), this.columnFamily(), this.properties.properties.getId()).kind(TableMetadata.Kind.VIEW).params(params);
                     this.add(baseTableMetadata, targetPartitionKeys, builder::addPartitionKeyColumn);
                     this.add(baseTableMetadata, targetClusteringColumns, builder::addClusteringColumn);
                     isNewView = ViewFeature.useNewMaterializedView();
                     viewColumns.regularColumns().stream().forEach((c) -> {
                        builder.addRegularColumn(c.name(), baseTableMetadata.getColumn(c.name()).type, isNewView && c.isRequiredForLiveness());
                     });
                     if(isNewView) {
                        viewColumns.hiddenColumns().stream().forEach((c) -> {
                           builder.addRegularColumn(c.hiddenName(), baseTableMetadata.getColumn(c.name()).type, c.isRequiredForLiveness(), true);
                        });
                     }

                     TableMetadata viewTableMetadata = builder.build();
                     ViewMetadata definition = new ViewMetadata(viewTableMetadata, baseTableMetadata, selectedColumns.isEmpty(), this.whereClause.relations, isNewView?ViewMetadata.ViewVersion.V1:ViewMetadata.ViewVersion.V0);
                     if(viewTableMetadata.hasRequiredColumnsForLiveness() || viewTableMetadata.columns().stream().anyMatch((c) -> {
                        return c.isHidden();
                     })) {
                        ViewFeature.validateViewVersion();
                     }

                     return MigrationManager.announceNewView(definition, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily()))).onErrorResumeNext((e) -> {
                        return e instanceof AlreadyExistsException && this.ifNotExists?Maybe.empty():Maybe.error(e);
                     });
                  }
               }
            }
         }
      }
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
