package org.apache.cassandra.schema;

import com.datastax.bdp.db.utils.concurrent.CompletableFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.hash.Hasher;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.RowIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaKeyspace {
   private static final Logger logger = LoggerFactory.getLogger(SchemaKeyspace.class);
   private static final boolean FLUSH_SCHEMA_TABLES = PropertyConfiguration.getBoolean("cassandra.test.flush_local_schema_changes", true);
   private static final boolean IGNORE_CORRUPTED_SCHEMA_TABLES = PropertyConfiguration.getBoolean("cassandra.ignore_corrupted_schema_tables", false);
   public static final String KEYSPACES = "keyspaces";
   public static final String TABLES = "tables";
   public static final String COLUMNS = "columns";
   public static final String HIDDEN_COLUMNS = "hidden_columns";
   public static final String DROPPED_COLUMNS = "dropped_columns";
   public static final String TRIGGERS = "triggers";
   public static final String VIEWS = "views";
   public static final String TYPES = "types";
   public static final String FUNCTIONS = "functions";
   public static final String AGGREGATES = "aggregates";
   public static final String INDEXES = "indexes";
   public static final List<String> ALL = UnmodifiableArrayList.of((Object[])(new String[]{"columns", "hidden_columns", "dropped_columns", "triggers", "types", "functions", "aggregates", "indexes", "tables", "views", "keyspaces"}));
   public static final List<String> ALL_REVERSED;
   private static final Set<String> TABLES_WITH_CDC_ADDED;
   private static final TableMetadata Keyspaces;
   private static final TableMetadata Tables;
   private static final TableMetadata Columns;
   private static final TableMetadata ViewColumns;
   private static final TableMetadata DroppedColumns;
   private static final TableMetadata Triggers;
   private static final TableMetadata Views;
   private static final TableMetadata Indexes;
   private static final TableMetadata Types;
   private static final TableMetadata Functions;
   private static final TableMetadata Aggregates;
   private static final List<TableMetadata> ALL_TABLE_METADATA;

   private SchemaKeyspace() {
   }

   private static TableMetadata parse(String name, String description, String cql) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{name}), "system_schema").id(TableId.forSystemTable("system_schema", name)).dcLocalReadRepairChance(0.0D).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(7L)).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).comment(description).build();
   }

   public static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("system_schema", KeyspaceParams.local(), Tables.of((Iterable)ALL_TABLE_METADATA));
   }

   public static void saveSystemKeyspacesSchema() {
      KeyspaceMetadata system = Schema.instance.getKeyspaceMetadata("system");
      KeyspaceMetadata schema = Schema.instance.getKeyspaceMetadata("system_schema");
      long timestamp = ApolloTime.systemClockMicros();
      Iterator var4 = ALL.iterator();

      while(var4.hasNext()) {
         String schemaTable = (String)var4.next();
         String query = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE keyspace_name = ?", new Object[]{"system_schema", schemaTable});
         Iterator var7 = SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES.iterator();

         while(var7.hasNext()) {
            String systemKeyspace = (String)var7.next();
            QueryProcessor.executeOnceInternal(query, new Object[]{Long.valueOf(timestamp), systemKeyspace}).blockingGet();
         }
      }

      makeCreateKeyspaceMutation(system, timestamp + 1L).build().apply();
      makeCreateKeyspaceMutation(schema, timestamp + 1L).build().apply();
   }

   public static void truncate() {
      ALL_REVERSED.forEach((table) -> {
         getSchemaCFS(table).truncateBlocking();
      });
   }

   static CompletableFuture<Void> flush() {
      return DatabaseDescriptor.isUnsafeSystem()?CompletableFuture.completedFuture((Object)null):CompletableFutures.allOf((Collection)Lists.transform(ALL, (t) -> {
         return getSchemaCFS(t).forceFlush(ColumnFamilyStore.FlushReason.UNKNOWN);
      }));
   }

   static UUID calculateSchemaDigest() {
      Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
      Iterator var1 = ALL.iterator();

      while(true) {
         String table;
         do {
            if(!var1.hasNext()) {
               return UUID.nameUUIDFromBytes(hasher.hash().asBytes());
            }

            table = (String)var1.next();
         } while(table.equals("dropped_columns"));

         ReadCommand cmd = getReadCommandForTableSchema(table);
         PartitionIterator schema = FlowablePartitions.toPartitionsFiltered(cmd.executeInternal());
         Throwable var5 = null;

         try {
            while(schema.hasNext()) {
               RowIterator partition = (RowIterator)schema.next();
               Throwable var7 = null;

               try {
                  if(partition != null && !isSystemKeyspaceSchemaPartition(partition.partitionKey())) {
                     RowIterators.digest(partition, hasher);
                  }
               } catch (Throwable var32) {
                  var7 = var32;
                  throw var32;
               } finally {
                  if(partition != null) {
                     if(var7 != null) {
                        try {
                           partition.close();
                        } catch (Throwable var31) {
                           var7.addSuppressed(var31);
                        }
                     } else {
                        partition.close();
                     }
                  }

               }
            }
         } catch (Throwable var34) {
            var5 = var34;
            throw var34;
         } finally {
            if(schema != null) {
               if(var5 != null) {
                  try {
                     schema.close();
                  } catch (Throwable var30) {
                     var5.addSuppressed(var30);
                  }
               } else {
                  schema.close();
               }
            }

         }
      }
   }

   private static ColumnFamilyStore getSchemaCFS(String schemaTableName) {
      return Keyspace.open("system_schema").getColumnFamilyStore(schemaTableName);
   }

   private static ReadCommand getReadCommandForTableSchema(String schemaTableName) {
      ColumnFamilyStore cfs = getSchemaCFS(schemaTableName);
      return PartitionRangeReadCommand.allDataRead(cfs.metadata(), ApolloTime.systemClockSecondsAsInt());
   }

   static synchronized SchemaMigration convertSchemaToMutations() {
      Map<DecoratedKey, Mutation> mutationMap = new HashMap();
      Iterator var1 = ALL.iterator();

      while(var1.hasNext()) {
         String table = (String)var1.next();
         convertSchemaToMutations(mutationMap, table);
      }

      return SchemaMigration.schema(mutationMap.values());
   }

   private static void convertSchemaToMutations(Map<DecoratedKey, Mutation> mutationMap, String schemaTableName) {
      ReadCommand cmd = getReadCommandForTableSchema(schemaTableName);

      try {
         UnfilteredPartitionIterator iter = FlowablePartitions.toPartitions(cmd.executeLocally(), cmd.metadata());
         Throwable var4 = null;

         try {
            while(iter.hasNext()) {
               UnfilteredRowIterator partition = (UnfilteredRowIterator)iter.next();
               Throwable var6 = null;

               try {
                  if(!isSystemKeyspaceSchemaPartition(partition.partitionKey())) {
                     DecoratedKey key = partition.partitionKey();
                     Mutation mutation = (Mutation)mutationMap.computeIfAbsent(key, (k) -> {
                        cmd.metadata().partitionKeyType.validate(key.getKey());
                        return new Mutation("system_schema", key);
                     });
                     mutation.add(makeUpdateForSchema(UnfilteredRowIterators.withValidation(partition), cmd.columnFilter()));
                  }
               } catch (Throwable var34) {
                  var6 = var34;
                  throw var34;
               } finally {
                  if(partition != null) {
                     if(var6 != null) {
                        try {
                           partition.close();
                        } catch (Throwable var33) {
                           var6.addSuppressed(var33);
                        }
                     } else {
                        partition.close();
                     }
                  }

               }
            }
         } catch (Throwable var36) {
            var4 = var36;
            throw var36;
         } finally {
            if(iter != null) {
               if(var4 != null) {
                  try {
                     iter.close();
                  } catch (Throwable var32) {
                     var4.addSuppressed(var32);
                  }
               } else {
                  iter.close();
               }
            }

         }
      } catch (Exception var38) {
         logger.error("Can't convert a schema entry to mutation", var38);
      }

   }

   private static PartitionUpdate makeUpdateForSchema(UnfilteredRowIterator partition, ColumnFilter filter) {
      if(!DatabaseDescriptor.isCDCEnabled() && TABLES_WITH_CDC_ADDED.contains(partition.metadata().name)) {
         ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(partition.metadata());
         Iterator var3 = filter.fetchedColumns().iterator();

         while(var3.hasNext()) {
            ColumnMetadata column = (ColumnMetadata)var3.next();
            if(!column.name.toString().equals("cdc")) {
               builder.add(column);
            }
         }

         return PartitionUpdate.fromIterator(partition, builder.build());
      } else {
         return PartitionUpdate.fromIterator(partition, filter);
      }
   }

   private static boolean isSystemKeyspaceSchemaPartition(DecoratedKey partitionKey) {
      return SchemaConstants.isLocalSystemKeyspace((String)UTF8Type.instance.compose(partitionKey.getKey()));
   }

   private static DecoratedKey decorate(TableMetadata metadata, Object value) {
      return metadata.partitioner.decorateKey(metadata.partitionKeyType.decompose(value));
   }

   static Mutation.SimpleBuilder makeCreateKeyspaceMutation(String name, KeyspaceParams params, long timestamp) {
      Mutation.SimpleBuilder builder = Mutation.simpleBuilder(Keyspaces.keyspace, decorate(Keyspaces, name)).timestamp(timestamp);
      builder.update(Keyspaces).row(new Object[0]).add(KeyspaceParams.Option.DURABLE_WRITES.toString(), Boolean.valueOf(params.durableWrites)).add(KeyspaceParams.Option.REPLICATION.toString(), params.replication.asMap());
      return builder;
   }

   static Mutation.SimpleBuilder makeCreateKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      keyspace.tables.forEach((table) -> {
         addTableToSchemaMutation(table, true, builder);
      });
      keyspace.views.forEach((view) -> {
         addViewToSchemaMutation(view, true, builder);
      });
      keyspace.types.forEach((type) -> {
         addTypeToSchemaMutation(type, builder);
      });
      keyspace.functions.udfs().forEach((udf) -> {
         addFunctionToSchemaMutation(udf, builder);
      });
      keyspace.functions.udas().forEach((uda) -> {
         addAggregateToSchemaMutation(uda, builder);
      });
      return builder;
   }

   static Mutation.SimpleBuilder makeDropKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp) {
      Mutation.SimpleBuilder builder = Mutation.simpleBuilder("system_schema", decorate(Keyspaces, keyspace.name)).timestamp(timestamp);
      Iterator var4 = ALL_TABLE_METADATA.iterator();

      while(var4.hasNext()) {
         TableMetadata schemaTable = (TableMetadata)var4.next();
         builder.update(schemaTable).delete();
      }

      return builder;
   }

   static Mutation.SimpleBuilder makeCreateTypeMutation(KeyspaceMetadata keyspace, UserType type, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      addTypeToSchemaMutation(type, builder);
      return builder;
   }

   static void addTypeToSchemaMutation(UserType type, Mutation.SimpleBuilder mutation) {
      mutation.update(Types).row(new Object[]{type.getNameAsString()}).add("field_names", type.fieldNames().stream().map(FieldIdentifier::toString).collect(Collectors.toList())).add("field_types", type.fieldTypes().stream().map(AbstractType::asCQL3Type).map(Object::toString).collect(Collectors.toList()));
   }

   static Mutation.SimpleBuilder dropTypeFromSchemaMutation(KeyspaceMetadata keyspace, UserType type, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      builder.update(Types).row(new Object[]{type.name}).delete();
      return builder;
   }

   static Mutation.SimpleBuilder makeCreateTableMutation(KeyspaceMetadata keyspace, TableMetadata table, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      addTableToSchemaMutation(table, true, builder);
      return builder;
   }

   static void addTableToSchemaMutation(TableMetadata table, boolean withColumnsAndTriggers, Mutation.SimpleBuilder builder) {
      Row.SimpleBuilder rowBuilder = builder.update(Tables).row(new Object[]{table.name}).add("id", table.id.asUUID()).add("flags", TableMetadata.Flag.toStringSet(table.flags));
      if(withColumnsAndTriggers) {
         Iterator var4 = table.columns().iterator();

         while(var4.hasNext()) {
            ColumnMetadata column = (ColumnMetadata)var4.next();
            addColumnToSchemaMutation(table, column, builder);
         }

         UnmodifiableIterator var6 = table.droppedColumns.values().iterator();

         while(var6.hasNext()) {
            DroppedColumn column = (DroppedColumn)var6.next();
            addDroppedColumnToSchemaMutation(table, column, builder);
         }

         var4 = table.triggers.iterator();

         while(var4.hasNext()) {
            TriggerMetadata trigger = (TriggerMetadata)var4.next();
            addTriggerToSchemaMutation(table, trigger, builder);
         }

         var4 = table.indexes.iterator();

         while(var4.hasNext()) {
            IndexMetadata index = (IndexMetadata)var4.next();
            addIndexToSchemaMutation(table, index, builder);
         }
      }

      addTableParamsToRowBuilder(table.params, rowBuilder);
   }

   private static void addTableParamsToRowBuilder(TableParams params, Row.SimpleBuilder builder) {
      builder.add("bloom_filter_fp_chance", Double.valueOf(params.bloomFilterFpChance)).add("caching", params.caching.asMap()).add("comment", params.comment).add("compaction", params.compaction.asMap()).add("compression", params.compression.asMap()).add("crc_check_chance", Double.valueOf(params.crcCheckChance)).add("dclocal_read_repair_chance", Double.valueOf(params.dcLocalReadRepairChance)).add("default_time_to_live", Integer.valueOf(params.defaultTimeToLive)).add("extensions", params.extensions).add("gc_grace_seconds", Integer.valueOf(params.gcGraceSeconds)).add("max_index_interval", Integer.valueOf(params.maxIndexInterval)).add("memtable_flush_period_in_ms", Integer.valueOf(params.memtableFlushPeriodInMs)).add("min_index_interval", Integer.valueOf(params.minIndexInterval)).add("read_repair_chance", Double.valueOf(params.readRepairChance)).add("speculative_retry", params.speculativeRetry.toString());
      if(DatabaseDescriptor.isCDCEnabled()) {
         builder.add("cdc", Boolean.valueOf(params.cdc));
      }

      Map<String, String> nodeSyncParams = params.nodeSync.asMap();
      if(!nodeSyncParams.isEmpty()) {
         builder.add("nodesync", nodeSyncParams);
      }

   }

   static Mutation.SimpleBuilder makeUpdateTableMutation(KeyspaceMetadata keyspace, TableMetadata oldTable, TableMetadata newTable, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      addTableToSchemaMutation(newTable, false, builder);
      MapDifference<ByteBuffer, ColumnMetadata> columnDiff = Maps.difference(oldTable.columns, newTable.columns);
      Iterator var7 = columnDiff.entriesOnlyOnLeft().values().iterator();

      ColumnMetadata column;
      while(var7.hasNext()) {
         column = (ColumnMetadata)var7.next();
         dropColumnFromSchemaMutation(oldTable, column, builder);
      }

      var7 = columnDiff.entriesOnlyOnRight().values().iterator();

      while(var7.hasNext()) {
         column = (ColumnMetadata)var7.next();
         addColumnToSchemaMutation(newTable, column, builder);
      }

      var7 = columnDiff.entriesDiffering().keySet().iterator();

      while(var7.hasNext()) {
         ByteBuffer name = (ByteBuffer)var7.next();
         addColumnToSchemaMutation(newTable, newTable.getColumn(name), builder);
      }

      MapDifference<ByteBuffer, DroppedColumn> droppedColumnDiff = Maps.difference(oldTable.droppedColumns, newTable.droppedColumns);
      Iterator var14 = droppedColumnDiff.entriesOnlyOnRight().values().iterator();

      while(var14.hasNext()) {
         DroppedColumn column = (DroppedColumn)var14.next();
         addDroppedColumnToSchemaMutation(newTable, column, builder);
      }

      var14 = droppedColumnDiff.entriesDiffering().keySet().iterator();

      while(var14.hasNext()) {
         ByteBuffer name = (ByteBuffer)var14.next();
         addDroppedColumnToSchemaMutation(newTable, (DroppedColumn)newTable.droppedColumns.get(name), builder);
      }

      MapDifference<String, TriggerMetadata> triggerDiff = triggersDiff(oldTable.triggers, newTable.triggers);
      Iterator var17 = triggerDiff.entriesOnlyOnLeft().values().iterator();

      TriggerMetadata trigger;
      while(var17.hasNext()) {
         trigger = (TriggerMetadata)var17.next();
         dropTriggerFromSchemaMutation(oldTable, trigger, builder);
      }

      var17 = triggerDiff.entriesOnlyOnRight().values().iterator();

      while(var17.hasNext()) {
         trigger = (TriggerMetadata)var17.next();
         addTriggerToSchemaMutation(newTable, trigger, builder);
      }

      MapDifference<String, IndexMetadata> indexesDiff = indexesDiff(oldTable.indexes, newTable.indexes);
      Iterator var19 = indexesDiff.entriesOnlyOnLeft().values().iterator();

      IndexMetadata index;
      while(var19.hasNext()) {
         index = (IndexMetadata)var19.next();
         dropIndexFromSchemaMutation(oldTable, index, builder);
      }

      var19 = indexesDiff.entriesOnlyOnRight().values().iterator();

      while(var19.hasNext()) {
         index = (IndexMetadata)var19.next();
         addIndexToSchemaMutation(newTable, index, builder);
      }

      var19 = indexesDiff.entriesDiffering().values().iterator();

      while(var19.hasNext()) {
         ValueDifference<IndexMetadata> diff = (ValueDifference)var19.next();
         addUpdatedIndexToSchemaMutation(newTable, (IndexMetadata)diff.rightValue(), builder);
      }

      return builder;
   }

   private static MapDifference<String, IndexMetadata> indexesDiff(Indexes before, Indexes after) {
      Map<String, IndexMetadata> beforeMap = new HashMap();
      before.forEach((i) -> {
         IndexMetadata var10000 = (IndexMetadata)beforeMap.put(i.name, i);
      });
      Map<String, IndexMetadata> afterMap = new HashMap();
      after.forEach((i) -> {
         IndexMetadata var10000 = (IndexMetadata)afterMap.put(i.name, i);
      });
      return Maps.difference(beforeMap, afterMap);
   }

   private static MapDifference<String, TriggerMetadata> triggersDiff(Triggers before, Triggers after) {
      Map<String, TriggerMetadata> beforeMap = new HashMap();
      before.forEach((t) -> {
         TriggerMetadata var10000 = (TriggerMetadata)beforeMap.put(t.name, t);
      });
      Map<String, TriggerMetadata> afterMap = new HashMap();
      after.forEach((t) -> {
         TriggerMetadata var10000 = (TriggerMetadata)afterMap.put(t.name, t);
      });
      return Maps.difference(beforeMap, afterMap);
   }

   static Mutation.SimpleBuilder makeDropTableMutation(KeyspaceMetadata keyspace, TableMetadata table, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      builder.update(Tables).row(new Object[]{table.name}).delete();
      Iterator var5 = table.columns().iterator();

      while(var5.hasNext()) {
         ColumnMetadata column = (ColumnMetadata)var5.next();
         dropColumnFromSchemaMutation(table, column, builder);
      }

      UnmodifiableIterator var7 = table.droppedColumns.values().iterator();

      while(var7.hasNext()) {
         DroppedColumn column = (DroppedColumn)var7.next();
         dropDroppedColumnFromSchemaMutation(table, column, builder);
      }

      var5 = table.triggers.iterator();

      while(var5.hasNext()) {
         TriggerMetadata trigger = (TriggerMetadata)var5.next();
         dropTriggerFromSchemaMutation(table, trigger, builder);
      }

      var5 = table.indexes.iterator();

      while(var5.hasNext()) {
         IndexMetadata index = (IndexMetadata)var5.next();
         dropIndexFromSchemaMutation(table, index, builder);
      }

      return builder;
   }

   private static void addColumnToSchemaMutation(TableMetadata table, ColumnMetadata column, Mutation.SimpleBuilder builder) {
      AbstractType<?> type = column.type;
      if(type instanceof ReversedType) {
         type = ((ReversedType)type).baseType;
      }

      TableMetadata tableToUpdate = column.isHidden?ViewColumns:Columns;
      builder.update(tableToUpdate).row(new Object[]{table.name, column.name.toString()}).add("column_name_bytes", column.name.bytes).add("kind", column.kind.toString().toLowerCase()).add("position", Integer.valueOf(column.position())).add("clustering_order", column.clusteringOrder().toString().toLowerCase()).add("type", type.asCQL3Type().toString()).add("required_for_liveness", Boolean.valueOf(column.isRequiredForLiveness));
   }

   private static void dropColumnFromSchemaMutation(TableMetadata table, ColumnMetadata column, Mutation.SimpleBuilder builder) {
      TableMetadata tableToUpdate = column.isHidden?ViewColumns:Columns;
      builder.update(tableToUpdate).row(new Object[]{table.name, column.name.toString()}).delete();
   }

   private static void addDroppedColumnToSchemaMutation(TableMetadata table, DroppedColumn column, Mutation.SimpleBuilder builder) {
      builder.update(DroppedColumns).row(new Object[]{table.name, column.column.name.toString()}).add("dropped_time", new Date(TimeUnit.MICROSECONDS.toMillis(column.droppedTime))).add("type", expandUserTypes(column.column.type).asCQL3Type().toString()).add("kind", column.column.kind.toString().toLowerCase());
   }

   private static void dropDroppedColumnFromSchemaMutation(TableMetadata table, DroppedColumn column, Mutation.SimpleBuilder builder) {
      builder.update(DroppedColumns).row(new Object[]{table.name, column.column.name.toString()}).delete();
   }

   private static void addTriggerToSchemaMutation(TableMetadata table, TriggerMetadata trigger, Mutation.SimpleBuilder builder) {
      builder.update(Triggers).row(new Object[]{table.name, trigger.name}).add("options", Collections.singletonMap("class", trigger.classOption));
   }

   private static void dropTriggerFromSchemaMutation(TableMetadata table, TriggerMetadata trigger, Mutation.SimpleBuilder builder) {
      builder.update(Triggers).row(new Object[]{table.name, trigger.name}).delete();
   }

   static Mutation.SimpleBuilder makeCreateViewMutation(KeyspaceMetadata keyspace, ViewMetadata view, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      addViewToSchemaMutation(view, true, builder);
      return builder;
   }

   private static void addViewToSchemaMutation(ViewMetadata view, boolean includeColumns, Mutation.SimpleBuilder builder) {
      TableMetadata table = view.viewTableMetadata;
      Row.SimpleBuilder rowBuilder = builder.update(Views).row(new Object[]{view.name}).add("include_all_columns", Boolean.valueOf(view.includeAllColumns)).add("base_table_id", view.baseTableId().asUUID()).add("base_table_name", view.baseTableName()).add("where_clause", view.whereClause).add("id", table.id.asUUID()).add("version", Integer.valueOf(view.getVersion().ordinal()));
      addTableParamsToRowBuilder(table.params, rowBuilder);
      if(includeColumns) {
         Iterator var5 = table.columns().iterator();

         while(var5.hasNext()) {
            ColumnMetadata column = (ColumnMetadata)var5.next();
            addColumnToSchemaMutation(table, column, builder);
         }

         UnmodifiableIterator var7 = table.droppedColumns.values().iterator();

         while(var7.hasNext()) {
            DroppedColumn column = (DroppedColumn)var7.next();
            addDroppedColumnToSchemaMutation(table, column, builder);
         }
      }

   }

   static Mutation.SimpleBuilder makeDropViewMutation(KeyspaceMetadata keyspace, ViewMetadata view, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      builder.update(Views).row(new Object[]{view.name}).delete();
      TableMetadata table = view.viewTableMetadata;
      Iterator var6 = table.columns().iterator();

      while(var6.hasNext()) {
         ColumnMetadata column = (ColumnMetadata)var6.next();
         dropColumnFromSchemaMutation(table, column, builder);
      }

      UnmodifiableIterator var8 = table.droppedColumns.values().iterator();

      while(var8.hasNext()) {
         DroppedColumn column = (DroppedColumn)var8.next();
         dropDroppedColumnFromSchemaMutation(table, column, builder);
      }

      var6 = table.indexes.iterator();

      while(var6.hasNext()) {
         IndexMetadata index = (IndexMetadata)var6.next();
         dropIndexFromSchemaMutation(table, index, builder);
      }

      return builder;
   }

   static Mutation.SimpleBuilder makeUpdateViewMutation(Mutation.SimpleBuilder builder, ViewMetadata oldView, ViewMetadata newView) {
      addViewToSchemaMutation(newView, false, builder);
      MapDifference<ByteBuffer, ColumnMetadata> columnDiff = Maps.difference(oldView.viewTableMetadata.columns, newView.viewTableMetadata.columns);
      Iterator var4 = columnDiff.entriesOnlyOnLeft().values().iterator();

      ColumnMetadata column;
      while(var4.hasNext()) {
         column = (ColumnMetadata)var4.next();
         dropColumnFromSchemaMutation(oldView.viewTableMetadata, column, builder);
      }

      var4 = columnDiff.entriesOnlyOnRight().values().iterator();

      while(var4.hasNext()) {
         column = (ColumnMetadata)var4.next();
         addColumnToSchemaMutation(newView.viewTableMetadata, column, builder);
      }

      var4 = columnDiff.entriesDiffering().keySet().iterator();

      while(var4.hasNext()) {
         ByteBuffer name = (ByteBuffer)var4.next();
         addColumnToSchemaMutation(newView.viewTableMetadata, newView.viewTableMetadata.getColumn(name), builder);
      }

      MapDifference<ByteBuffer, DroppedColumn> droppedColumnDiff = Maps.difference(oldView.viewTableMetadata.droppedColumns, newView.viewTableMetadata.droppedColumns);
      Iterator var9 = droppedColumnDiff.entriesOnlyOnRight().values().iterator();

      while(var9.hasNext()) {
         DroppedColumn column = (DroppedColumn)var9.next();
         addDroppedColumnToSchemaMutation(oldView.viewTableMetadata, column, builder);
      }

      var9 = droppedColumnDiff.entriesDiffering().keySet().iterator();

      while(var9.hasNext()) {
         ByteBuffer name = (ByteBuffer)var9.next();
         addDroppedColumnToSchemaMutation(newView.viewTableMetadata, (DroppedColumn)newView.viewTableMetadata.droppedColumns.get(name), builder);
      }

      return builder;
   }

   private static void addIndexToSchemaMutation(TableMetadata table, IndexMetadata index, Mutation.SimpleBuilder builder) {
      builder.update(Indexes).row(new Object[]{table.name, index.name}).add("kind", index.kind.toString()).add("options", index.options);
   }

   private static void dropIndexFromSchemaMutation(TableMetadata table, IndexMetadata index, Mutation.SimpleBuilder builder) {
      builder.update(Indexes).row(new Object[]{table.name, index.name}).delete();
   }

   private static void addUpdatedIndexToSchemaMutation(TableMetadata table, IndexMetadata index, Mutation.SimpleBuilder builder) {
      addIndexToSchemaMutation(table, index, builder);
   }

   static Mutation.SimpleBuilder makeCreateFunctionMutation(KeyspaceMetadata keyspace, UDFunction function, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      addFunctionToSchemaMutation(function, builder);
      return builder;
   }

   static void addFunctionToSchemaMutation(UDFunction function, Mutation.SimpleBuilder builder) {
      builder.update(Functions).row(new Object[]{function.name().name, function.argumentsList()}).add("body", function.body()).add("language", function.language()).add("return_type", function.returnType().asCQL3Type().toString()).add("called_on_null_input", Boolean.valueOf(function.isCalledOnNullInput())).add("argument_names", function.argNames().stream().map((c) -> {
         return bbToString(c.bytes);
      }).collect(Collectors.toList())).add("deterministic", Boolean.valueOf(function.isDeterministic())).add("monotonic", Boolean.valueOf(function.isMonotonic())).add("monotonic_on", function.monotonicOn().stream().map((c) -> {
         return bbToString(c.bytes);
      }).collect(Collectors.toList()));
   }

   private static String bbToString(ByteBuffer bb) {
      try {
         return ByteBufferUtil.string(bb);
      } catch (CharacterCodingException var2) {
         throw new RuntimeException(var2);
      }
   }

   static Mutation.SimpleBuilder makeDropFunctionMutation(KeyspaceMetadata keyspace, UDFunction function, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      builder.update(Functions).row(new Object[]{function.name().name, function.argumentsList()}).delete();
      return builder;
   }

   static Mutation.SimpleBuilder makeCreateAggregateMutation(KeyspaceMetadata keyspace, UDAggregate aggregate, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      addAggregateToSchemaMutation(aggregate, builder);
      return builder;
   }

   static void addAggregateToSchemaMutation(UDAggregate aggregate, Mutation.SimpleBuilder builder) {
      builder.update(Aggregates).row(new Object[]{aggregate.name().name, aggregate.argumentsList()}).add("return_type", aggregate.returnType().asCQL3Type().toString()).add("state_func", aggregate.stateFunction().name().name).add("state_type", aggregate.stateType().asCQL3Type().toString()).add("final_func", aggregate.finalFunction() != null?aggregate.finalFunction().name().name:null).add("deterministic", Boolean.valueOf(aggregate.isDeterministic())).add("initcond", aggregate.initialCondition() != null?aggregate.stateType().freeze().asCQL3Type().toCQLLiteral(aggregate.initialCondition(), ProtocolVersion.CURRENT):null);
   }

   static Mutation.SimpleBuilder makeDropAggregateMutation(KeyspaceMetadata keyspace, UDAggregate aggregate, long timestamp) {
      Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
      builder.update(Aggregates).row(new Object[]{aggregate.name().name, aggregate.argumentsList()}).delete();
      return builder;
   }

   static Keyspaces fetchNonSystemKeyspaces() {
      return fetchKeyspacesWithout(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES);
   }

   public static void validateNonCompact() throws StartupException {
      String query = String.format("SELECT keyspace_name, table_name, flags FROM %s.%s", new Object[]{"system_schema", "tables"});
      String messages = "";
      Iterator var2 = ((UntypedResultSet)query(query, new Object[0]).blockingGet()).iterator();

      while(var2.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var2.next();
         if(!SchemaConstants.isLocalSystemKeyspace(row.getString("keyspace_name"))) {
            Set<String> flags = row.getSet("flags", AsciiType.instance);
            if(!TableMetadata.Flag.isCQLCompatible(TableMetadata.Flag.fromStringSet(flags))) {
               messages = messages + String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE;\n", new Object[]{ColumnIdentifier.maybeQuote(row.getString("keyspace_name")), ColumnIdentifier.maybeQuote(row.getString("table_name"))});
            }
         }
      }

      if(!messages.isEmpty()) {
         throw new StartupException(101, String.format("Compact Tables are not allowed in Cassandra starting with 4.0 version. In order to migrate off Compact Storage, downgrade to the latest DSE 5.0/5.1, start the node with `-Dcassandra.commitlog.ignorereplayerrors=true` passed on the command line or in jvm.options, and run the following commands: \n\n%s\nThen restart the node with the new DSE version without `-Dcassandra.commitlog.ignorereplayerrors=true`.", new Object[]{messages}));
      }
   }

   private static Keyspaces fetchKeyspacesWithout(Set<String> excludedKeyspaceNames) {
      String query = String.format("SELECT keyspace_name FROM %s.%s", new Object[]{"system_schema", "keyspaces"});
      Keyspaces.Builder keyspaces = Keyspaces.builder();
      Iterator var3 = ((UntypedResultSet)query(query, new Object[0]).blockingGet()).iterator();

      while(var3.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
         String keyspaceName = row.getString("keyspace_name");
         if(!excludedKeyspaceNames.contains(keyspaceName)) {
            keyspaces.add(fetchKeyspace(keyspaceName));
         }
      }

      return keyspaces.build();
   }

   private static KeyspaceMetadata fetchKeyspace(String keyspaceName) {
      KeyspaceParams params = fetchKeyspaceParams(keyspaceName);
      Types types = fetchTypes(keyspaceName);
      Tables tables = fetchTables(keyspaceName, types);
      Views views = fetchViews(keyspaceName, types, tables);
      Functions functions = fetchFunctions(keyspaceName, types);
      return KeyspaceMetadata.create(keyspaceName, params, tables, views, types, functions);
   }

   private static KeyspaceParams fetchKeyspaceParams(String keyspaceName) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "keyspaces"});
      UntypedResultSet.Row row = ((UntypedResultSet)query(query, new Object[]{keyspaceName}).blockingGet()).one();
      boolean durableWrites = row.getBoolean(KeyspaceParams.Option.DURABLE_WRITES.toString());
      Map<String, String> replication = row.getFrozenTextMap(KeyspaceParams.Option.REPLICATION.toString());
      return KeyspaceParams.create(durableWrites, replication);
   }

   private static Types fetchTypes(String keyspaceName) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "types"});
      Types.RawBuilder types = Types.rawBuilder(keyspaceName);
      Iterator var3 = ((UntypedResultSet)query(query, new Object[]{keyspaceName}).blockingGet()).iterator();

      while(var3.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
         String name = row.getString("type_name");
         List<String> fieldNames = row.getFrozenList("field_names", UTF8Type.instance);
         List<String> fieldTypes = row.getFrozenList("field_types", UTF8Type.instance);
         types.add(name, fieldNames, fieldTypes);
      }

      return types.build();
   }

   private static Tables fetchTables(String keyspaceName, Types types) {
      String query = String.format("SELECT table_name FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "tables"});
      Tables.Builder tables = Tables.builder();
      Set<String> tableSet = SetsFactory.newSet();
      Iterator var5 = ((UntypedResultSet)query(query, new Object[]{keyspaceName}).blockingGet()).iterator();

      while(var5.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var5.next();
         String tableName = null;

         try {
            tableName = row.getString("table_name");
            if(tableSet.contains(tableName)) {
               throw new SchemaKeyspace.DuplicateException(String.format("Found a duplicate entry for the table %s in keyspace %s.", new Object[]{tableName, keyspaceName}));
            }

            tables.add(fetchTable(keyspaceName, tableName, types));
            tableSet.add(tableName);
         } catch (SchemaKeyspace.MissingColumns | SchemaKeyspace.DuplicateException | MarshalException var10) {
            if(tableName == null) {
               if(!IGNORE_CORRUPTED_SCHEMA_TABLES) {
                  logger.error("Can not decode the table name in {} keyspace. This may be due to the sstable corruption.In order to start Cassandra ignoring this error, run it with -Dcassandra.ignore_corrupted_schema_tables=true", keyspaceName);
                  throw var10;
               }

               logger.warn("Skipping table in the keyspace {}, because cassandra.ignore_corrupted_schema_tables is set to true.", keyspaceName);
            } else {
               String errorMsg = String.format("No partition columns found for table %s.%s in %s.%s.  This may be due to corruption or concurrent dropping and altering of a table. If this table is supposed to be dropped, {}run the following query to cleanup: \"DELETE FROM %s.%s WHERE keyspace_name = '%s' AND table_name = '%s'; DELETE FROM %s.%s WHERE keyspace_name = '%s' AND table_name = '%s';\" If the table is not supposed to be dropped, restore %s.%s sstables from backups.", new Object[]{keyspaceName, tableName, "system_schema", "columns", "system_schema", "tables", keyspaceName, tableName, "system_schema", "columns", keyspaceName, tableName, "system_schema", "columns"});
               if(!IGNORE_CORRUPTED_SCHEMA_TABLES) {
                  logger.error(errorMsg, "restart cassandra with -Dcassandra.ignore_corrupted_schema_tables=true and ");
                  throw var10;
               }

               logger.warn(errorMsg, "", var10);
            }
         }
      }

      return tables.build();
   }

   private static TableMetadata fetchTable(String keyspaceName, String tableName, Types types) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", "tables"});
      UntypedResultSet rows = (UntypedResultSet)query(query, new Object[]{keyspaceName, tableName}).blockingGet();
      if(rows.isEmpty()) {
         throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", new Object[]{keyspaceName, tableName}));
      } else {
         UntypedResultSet.Row row = rows.one();
         return TableMetadata.builder(keyspaceName, tableName, TableId.fromUUID(row.getUUID("id"))).flags(TableMetadata.Flag.fromStringSet(row.getFrozenSet("flags", UTF8Type.instance))).params(createTableParamsFromRow(keyspaceName, tableName, row)).addColumns(fetchColumns(keyspaceName, tableName, types, false)).droppedColumns(fetchDroppedColumns(keyspaceName, tableName)).indexes(fetchIndexes(keyspaceName, tableName)).triggers(fetchTriggers(keyspaceName, tableName)).build();
      }
   }

   @VisibleForTesting
   static TableParams createTableParamsFromRow(String ksName, String tableName, UntypedResultSet.Row row) {
      return TableParams.builder().bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance")).caching(CachingParams.fromMap(row.getFrozenTextMap("caching"))).cdc(row.has("cdc") && row.getBoolean("cdc")).comment(row.getString("comment")).compaction(CompactionParams.fromMap(row.getFrozenTextMap("compaction"))).compression(CompressionParams.fromMap(row.getFrozenTextMap("compression"))).crcCheckChance(row.getDouble("crc_check_chance")).dcLocalReadRepairChance(row.getDouble("dclocal_read_repair_chance")).defaultTimeToLive(row.getInt("default_time_to_live")).extensions(row.getFrozenMap("extensions", UTF8Type.instance, BytesType.instance)).gcGraceSeconds(row.getInt("gc_grace_seconds")).maxIndexInterval(row.getInt("max_index_interval")).memtableFlushPeriodInMs(row.getInt("memtable_flush_period_in_ms")).minIndexInterval(row.getInt("min_index_interval")).nodeSync(NodeSyncParams.fromMap(ksName, tableName, row.getFrozenTextMap("nodesync"))).readRepairChance(row.getDouble("read_repair_chance")).speculativeRetry(SpeculativeRetryParam.fromString(row.getString("speculative_retry"))).build();
   }

   private static List<ColumnMetadata> fetchColumns(String keyspace, String table, Types types, boolean isHidden) {
      String tableName = isHidden?"hidden_columns":"columns";
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", tableName});
      UntypedResultSet columnRows = (UntypedResultSet)query(query, new Object[]{keyspace, table}).blockingGet();
      if(columnRows.isEmpty()) {
         if(isHidden) {
            return Collections.emptyList();
         } else {
            throw new SchemaKeyspace.MissingColumns("Columns not found in schema table for " + keyspace + "." + table);
         }
      } else {
         List<ColumnMetadata> columns = new ArrayList();
         columnRows.forEach((row) -> {
            columns.add(createColumnFromRow(row, types, isHidden));
         });
         if(!isHidden && columns.stream().noneMatch(ColumnMetadata::isPartitionKey)) {
            throw new SchemaKeyspace.MissingColumns("No partition key columns found in schema table for " + keyspace + "." + table);
         } else {
            return columns;
         }
      }
   }

   static ColumnMetadata createColumnFromRow(UntypedResultSet.Row row, Types types, boolean isHidden) {
      String keyspace = row.getString("keyspace_name");
      String table = row.getString("table_name");
      ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());
      int position = row.getInt("position");
      ColumnMetadata.ClusteringOrder order = ColumnMetadata.ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());
      AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), types);
      if(order == ColumnMetadata.ClusteringOrder.DESC) {
         type = ReversedType.getInstance((AbstractType)type);
      }

      ColumnIdentifier name = ColumnIdentifier.getInterned((AbstractType)type, row.getBytes("column_name_bytes"), row.getString("column_name"));
      boolean isRequiredForLiveness = row.has("required_for_liveness") && row.getBoolean("required_for_liveness");
      return new ColumnMetadata(keyspace, table, name, (AbstractType)type, position, kind, isRequiredForLiveness, isHidden);
   }

   private static Map<ByteBuffer, DroppedColumn> fetchDroppedColumns(String keyspace, String table) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", "dropped_columns"});
      Map<ByteBuffer, DroppedColumn> columns = new HashMap();
      Iterator var4 = ((UntypedResultSet)query(query, new Object[]{keyspace, table}).blockingGet()).iterator();

      while(var4.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var4.next();
         DroppedColumn column = createDroppedColumnFromRow(row);
         columns.put(column.column.name.bytes, column);
      }

      return columns;
   }

   private static DroppedColumn createDroppedColumnFromRow(UntypedResultSet.Row row) {
      String keyspace = row.getString("keyspace_name");
      String table = row.getString("table_name");
      String name = row.getString("column_name");
      AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), Types.none());
      ColumnMetadata.Kind kind = row.has("kind")?ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase()):ColumnMetadata.Kind.REGULAR;

      assert !kind.isPrimaryKeyKind() : "Unexpected dropped column kind: " + kind.toString();

      ColumnMetadata column = new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, -1, kind);
      long droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getLong("dropped_time"));
      return new DroppedColumn(column, droppedTime);
   }

   private static Indexes fetchIndexes(String keyspace, String table) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", "indexes"});
      Indexes.Builder indexes = Indexes.builder();
      ((UntypedResultSet)query(query, new Object[]{keyspace, table}).blockingGet()).forEach((row) -> {
         indexes.add(createIndexMetadataFromRow(row));
      });
      return indexes.build();
   }

   private static IndexMetadata createIndexMetadataFromRow(UntypedResultSet.Row row) {
      String name = row.getString("index_name");
      IndexMetadata.Kind type = IndexMetadata.Kind.valueOf(row.getString("kind"));
      Map<String, String> options = row.getFrozenTextMap("options");
      return IndexMetadata.fromSchemaMetadata(name, type, options);
   }

   private static Triggers fetchTriggers(String keyspace, String table) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", "triggers"});
      Triggers.Builder triggers = Triggers.builder();
      ((UntypedResultSet)query(query, new Object[]{keyspace, table}).blockingGet()).forEach((row) -> {
         triggers.add(createTriggerFromRow(row));
      });
      return triggers.build();
   }

   private static TriggerMetadata createTriggerFromRow(UntypedResultSet.Row row) {
      String name = row.getString("trigger_name");
      String classOption = (String)row.getFrozenTextMap("options").get("class");
      return new TriggerMetadata(name, classOption);
   }

   private static Views fetchViews(String keyspaceName, Types types, Tables tables) {
      String query = String.format("SELECT view_name FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "views"});
      Views.Builder views = Views.builder();
      Iterator var5 = ((UntypedResultSet)query(query, new Object[]{keyspaceName}).blockingGet()).iterator();

      while(var5.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var5.next();
         views.add(fetchView(keyspaceName, row.getString("view_name"), types, tables));
      }

      return views.build();
   }

   private static ViewMetadata fetchView(String keyspaceName, String viewName, Types types, Tables tables) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", new Object[]{"system_schema", "views"});
      UntypedResultSet rows = (UntypedResultSet)query(query, new Object[]{keyspaceName, viewName}).blockingGet();
      if(rows.isEmpty()) {
         throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", new Object[]{keyspaceName, viewName}));
      } else {
         UntypedResultSet.Row row = rows.one();
         TableId baseTableId = TableId.fromUUID(row.getUUID("base_table_id"));
         String baseTableName = row.getString("base_table_name");
         boolean includeAll = row.getBoolean("include_all_columns");
         String whereClause = row.getString("where_clause");
         ViewMetadata.ViewVersion version = row.has("version")?ViewMetadata.ViewVersion.values()[row.getInt("version")]:ViewMetadata.ViewVersion.V0;
         Optional<TableMetadata> baseTableMetadataOpt = StreamSupport.stream(tables.spliterator(), false).filter((t) -> {
            return t.id.equals(baseTableId);
         }).findFirst();
         if(!baseTableMetadataOpt.isPresent()) {
            throw new RuntimeException(String.format("Inconsistent state: view %s.%s present but base table %s.%s (%s) not present.", new Object[]{keyspaceName, viewName, keyspaceName, baseTableName, baseTableId.toString()}));
         } else {
            List<ColumnMetadata> columns = fetchColumns(keyspaceName, viewName, types, false);
            columns.addAll(fetchColumns(keyspaceName, viewName, types, true));
            TableMetadata viewTableMetadata = TableMetadata.builder(keyspaceName, viewName, TableId.fromUUID(row.getUUID("id"))).kind(TableMetadata.Kind.VIEW).addColumns(columns).droppedColumns(fetchDroppedColumns(keyspaceName, viewName)).params(createTableParamsFromRow(keyspaceName, viewName, row)).build();
            ViewMetadata viewMetadata = new ViewMetadata(viewTableMetadata, (TableMetadata)baseTableMetadataOpt.get(), includeAll, whereClause, version);
            viewTableMetadata.setViewMetadata(viewMetadata);
            return viewMetadata;
         }
      }
   }

   private static Functions fetchFunctions(String keyspaceName, Types types) {
      Functions udfs = fetchUDFs(keyspaceName, types);
      Functions udas = fetchUDAs(keyspaceName, udfs, types);
      return Functions.builder().add((Iterable)udfs).add((Iterable)udas).build();
   }

   private static Functions fetchUDFs(String keyspaceName, Types types) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "functions"});
      Functions.Builder functions = Functions.builder();
      Iterator var4 = ((UntypedResultSet)query(query, new Object[]{keyspaceName}).blockingGet()).iterator();

      while(var4.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var4.next();
         functions.add((org.apache.cassandra.cql3.functions.Function)createUDFFromRow(row, types));
      }

      return functions.build();
   }

   private static UDFunction createUDFFromRow(UntypedResultSet.Row row, Types types) {
      String ksName = row.getString("keyspace_name");
      String functionName = row.getString("function_name");
      FunctionName name = new FunctionName(ksName, functionName);
      List<ColumnIdentifier> argNames = new ArrayList();
      Iterator var6 = row.getFrozenList("argument_names", UTF8Type.instance).iterator();

      while(var6.hasNext()) {
         String arg = (String)var6.next();
         argNames.add(new ColumnIdentifier(arg, true));
      }

      List<AbstractType<?>> argTypes = new ArrayList();
      Iterator var18 = row.getFrozenList("argument_types", UTF8Type.instance).iterator();

      String language;
      while(var18.hasNext()) {
         language = (String)var18.next();
         argTypes.add(CQLTypeParser.parse(ksName, language, types));
      }

      AbstractType<?> returnType = CQLTypeParser.parse(ksName, row.getString("return_type"), types);
      language = row.getString("language");
      String body = row.getString("body");
      boolean calledOnNullInput = row.getBoolean("called_on_null_input");
      boolean deterministic = row.has("deterministic") && row.getBoolean("deterministic");
      boolean monotonic = row.has("monotonic") && row.getBoolean("monotonic");
      List<ColumnIdentifier> monotonicOn = row.has("monotonic_on")?(List)row.getFrozenList("monotonic_on", UTF8Type.instance).stream().map((arg) -> {
         return new ColumnIdentifier(arg, true);
      }).collect(Collectors.toList()):UnmodifiableArrayList.emptyList();
      org.apache.cassandra.cql3.functions.Function existing = (org.apache.cassandra.cql3.functions.Function)Schema.instance.findFunction(name, argTypes).orElse((Object)null);
      if(existing instanceof UDFunction) {
         UDFunction udf = (UDFunction)existing;
         if(udf.argNames().equals(argNames) && udf.argTypes().equals(argTypes) && udf.returnType().equals(returnType) && !udf.isAggregate() && udf.language().equals(language) && udf.body().equals(body) && udf.isCalledOnNullInput() == calledOnNullInput) {
            logger.trace("Skipping duplicate compilation of already existing UDF {}", name);
            return udf;
         }
      }

      try {
         return UDFunction.create(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, (List)monotonicOn);
      } catch (InvalidRequestException var16) {
         logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", new Object[]{name}), var16);
         return UDFunction.createBrokenFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, (List)monotonicOn, var16);
      }
   }

   private static Functions fetchUDAs(String keyspaceName, Functions udfs, Types types) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "aggregates"});
      Functions.Builder aggregates = Functions.builder();
      Iterator var5 = ((UntypedResultSet)query(query, new Object[]{keyspaceName}).blockingGet()).iterator();

      while(var5.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var5.next();
         aggregates.add((org.apache.cassandra.cql3.functions.Function)createUDAFromRow(row, udfs, types));
      }

      return aggregates.build();
   }

   private static UDAggregate createUDAFromRow(UntypedResultSet.Row row, Functions functions, Types types) {
      String ksName = row.getString("keyspace_name");
      String functionName = row.getString("aggregate_name");
      FunctionName name = new FunctionName(ksName, functionName);
      List<AbstractType<?>> argTypes = (List)row.getFrozenList("argument_types", UTF8Type.instance).stream().map((t) -> {
         return CQLTypeParser.parse(ksName, t, types);
      }).collect(Collectors.toList());
      AbstractType<?> returnType = CQLTypeParser.parse(ksName, row.getString("return_type"), types);
      FunctionName stateFunc = new FunctionName(ksName, row.getString("state_func"));
      FunctionName finalFunc = row.has("final_func")?new FunctionName(ksName, row.getString("final_func")):null;
      AbstractType<?> stateType = row.has("state_type")?CQLTypeParser.parse(ksName, row.getString("state_type"), types):null;
      ByteBuffer initcond = row.has("initcond")?Terms.asBytes(ksName, row.getString("initcond"), stateType):null;
      boolean deterministic = row.has("deterministic") && row.getBoolean("deterministic");

      try {
         return UDAggregate.create(functions, name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond, deterministic);
      } catch (InvalidRequestException var14) {
         return UDAggregate.createBroken(name, argTypes, returnType, initcond, deterministic, var14);
      }
   }

   private static Single<UntypedResultSet> query(String query, Object... variables) {
      return QueryProcessor.executeInternalAsync(query, variables);
   }

   static Set<String> affectedKeyspaces(Collection<Mutation> mutations) {
      return (Set)mutations.stream().map((m) -> {
         return (String)UTF8Type.instance.compose(m.key().getKey());
      }).collect(Collectors.toSet());
   }

   static synchronized void applyChanges(Collection<Mutation> mutations) {
      Completable.concat((Iterable)mutations.stream().map(Mutation::applyAsync).collect(Collectors.toList())).blockingAwait();
      if(FLUSH_SCHEMA_TABLES) {
         flush().join();
      }

   }

   static Keyspaces fetchKeyspaces(Set<String> toFetch) {
      String query = String.format("SELECT keyspace_name FROM %s.%s WHERE keyspace_name IN ?", new Object[]{"system_schema", "keyspaces"});
      Keyspaces.Builder keyspaces = Keyspaces.builder();
      Iterator var3 = ((UntypedResultSet)query(query, new Object[]{new ArrayList(toFetch)}).blockingGet()).iterator();

      while(var3.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
         keyspaces.add(fetchKeyspace(row.getString("keyspace_name")));
      }

      return keyspaces.build();
   }

   private static AbstractType<?> expandUserTypes(AbstractType<?> original) {
      if(original instanceof UserType) {
         UserType userType = (UserType)original;
         return new TupleType(expandUserTypes(userType.fieldTypes()), userType.isMultiCell());
      } else if(original instanceof TupleType) {
         return new TupleType(expandUserTypes(((TupleType)original).allTypes()));
      } else if(original instanceof ListType) {
         return ListType.getInstance(expandUserTypes(((ListType)original).getElementsType()), original.isMultiCell());
      } else if(original instanceof MapType) {
         MapType<?, ?> mt = (MapType)original;
         return MapType.getInstance(expandUserTypes(mt.getKeysType()), expandUserTypes(mt.getValuesType()), mt.isMultiCell());
      } else {
         return (AbstractType)(original instanceof SetType?SetType.getInstance(expandUserTypes(((SetType)original).getElementsType()), original.isMultiCell()):(original instanceof ReversedType?ReversedType.getInstance(expandUserTypes(((ReversedType)original).baseType)):(original instanceof CompositeType?CompositeType.getInstance(expandUserTypes(original.getComponents())):original)));
      }
   }

   private static List<AbstractType<?>> expandUserTypes(List<AbstractType<?>> types) {
      return (List)types.stream().map(SchemaKeyspace::expandUserTypes).collect(Collectors.toList());
   }

   static {
      ALL_REVERSED = UnmodifiableArrayList.reverseCopyOf(ALL);
      TABLES_WITH_CDC_ADDED = ImmutableSet.of("tables", "views");
      Keyspaces = parse("keyspaces", "keyspace definitions", "CREATE TABLE %s (keyspace_name text,durable_writes boolean,replication frozen<map<text, text>>,PRIMARY KEY ((keyspace_name)))");
      Tables = parse("tables", "table definitions", "CREATE TABLE %s (keyspace_name text,table_name text,bloom_filter_fp_chance double,caching frozen<map<text, text>>,cdc boolean,comment text,compaction frozen<map<text, text>>,compression frozen<map<text, text>>,crc_check_chance double,dclocal_read_repair_chance double,default_time_to_live int,extensions frozen<map<text, blob>>,flags frozen<set<text>>,gc_grace_seconds int,id uuid,max_index_interval int,memtable_flush_period_in_ms int,min_index_interval int,nodesync frozen<map<text, text>>,read_repair_chance double,speculative_retry text,PRIMARY KEY ((keyspace_name), table_name))");
      Columns = parse("columns", "column definitions", "CREATE TABLE %s (keyspace_name text,table_name text,column_name text,clustering_order text,column_name_bytes blob,kind text,position int,type text,required_for_liveness boolean,PRIMARY KEY ((keyspace_name), table_name, column_name))");
      ViewColumns = parse("hidden_columns", "hidden column definitions", "CREATE TABLE %s (keyspace_name text,table_name text,column_name text,clustering_order text,column_name_bytes blob,kind text,position int,type text,required_for_liveness boolean,PRIMARY KEY ((keyspace_name), table_name, column_name))");
      DroppedColumns = parse("dropped_columns", "dropped column registry", "CREATE TABLE %s (keyspace_name text,table_name text,column_name text,dropped_time timestamp,type text,kind text,PRIMARY KEY ((keyspace_name), table_name, column_name))");
      Triggers = parse("triggers", "trigger definitions", "CREATE TABLE %s (keyspace_name text,table_name text,trigger_name text,options frozen<map<text, text>>,PRIMARY KEY ((keyspace_name), table_name, trigger_name))");
      Views = parse("views", "view definitions", "CREATE TABLE %s (keyspace_name text,view_name text,base_table_id uuid,base_table_name text,where_clause text,bloom_filter_fp_chance double,caching frozen<map<text, text>>,cdc boolean,comment text,compaction frozen<map<text, text>>,compression frozen<map<text, text>>,crc_check_chance double,dclocal_read_repair_chance double,default_time_to_live int,extensions frozen<map<text, blob>>,gc_grace_seconds int,id uuid,include_all_columns boolean,max_index_interval int,memtable_flush_period_in_ms int,min_index_interval int,nodesync frozen<map<text, text>>,read_repair_chance double,speculative_retry text,version int,PRIMARY KEY ((keyspace_name), view_name))");
      Indexes = parse("indexes", "secondary index definitions", "CREATE TABLE %s (keyspace_name text,table_name text,index_name text,kind text,options frozen<map<text, text>>,PRIMARY KEY ((keyspace_name), table_name, index_name))");
      Types = parse("types", "user defined type definitions", "CREATE TABLE %s (keyspace_name text,type_name text,field_names frozen<list<text>>,field_types frozen<list<text>>,PRIMARY KEY ((keyspace_name), type_name))");
      Functions = parse("functions", "user defined function definitions", "CREATE TABLE %s (keyspace_name text,function_name text,argument_types frozen<list<text>>,argument_names frozen<list<text>>,body text,language text,return_type text,called_on_null_input boolean,deterministic boolean,monotonic boolean,monotonic_on frozen<list<text>>,PRIMARY KEY ((keyspace_name), function_name, argument_types))");
      Aggregates = parse("aggregates", "user defined aggregate definitions", "CREATE TABLE %s (keyspace_name text,aggregate_name text,argument_types frozen<list<text>>,final_func text,initcond text,return_type text,state_func text,state_type text,deterministic boolean,PRIMARY KEY ((keyspace_name), aggregate_name, argument_types))");
      ALL_TABLE_METADATA = UnmodifiableArrayList.of((Object[])(new TableMetadata[]{Keyspaces, Tables, Columns, ViewColumns, Triggers, DroppedColumns, Views, Types, Functions, Aggregates, Indexes}));
   }

   private static class DuplicateException extends RuntimeException {
      DuplicateException(String message) {
         super(message);
      }
   }

   @VisibleForTesting
   static class MissingColumns extends RuntimeException {
      MissingColumns(String message) {
         super(message);
      }
   }
}
