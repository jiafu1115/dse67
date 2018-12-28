package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public final class VirtualSchemaKeyspace extends VirtualKeyspace implements SchemaChangeListener {
   public static final String NAME = "system_virtual_schema";
   public static final String KEYSPACES = "keyspaces";
   public static final String TABLES = "tables";
   public static final String COLUMNS = "columns";
   public static final VirtualSchemaKeyspace instance = new VirtualSchemaKeyspace();
   public static final List<String> ALL = UnmodifiableArrayList.of("keyspaces", "tables", "columns");

   private VirtualSchemaKeyspace() {
      super("system_virtual_schema", ImmutableList.of(new VirtualSchemaKeyspace.VirtualKeyspaces("system_virtual_schema"), new VirtualSchemaKeyspace.VirtualTables("system_virtual_schema"), new VirtualSchemaKeyspace.VirtualColumns("system_virtual_schema")));
      this.addKeyspaceDataToTables(this.metadata());
   }

   public void onCreateVirtualKeyspace(String keyspaceName) {
      KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
      this.addKeyspaceDataToTables(keyspace);
   }

   private void addKeyspaceDataToTables(KeyspaceMetadata keyspace) {
      Iterator var2 = this.tables().iterator();

      while(var2.hasNext()) {
         VirtualTable table = (VirtualTable)var2.next();
         ((InMemoryVirtualTable)table).addRowData(keyspace);
      }

   }

   private static final class VirtualColumns extends InMemoryVirtualTable<KeyspaceMetadata> {
      private static final String KEYSPACE_NAME = "keyspace_name";
      private static final String TABLE_NAME = "table_name";
      private static final String COLUMN_NAME = "column_name";
      private static final String CLUSTERING_ORDER = "clustering_order";
      private static final String COLUMN_NAME_BYTES = "column_name_bytes";
      private static final String KIND = "kind";
      private static final String POSITION = "position";
      private static final String TYPE = "type";

      private VirtualColumns(String keyspace) {
         super(TableMetadata.builder(keyspace, "columns").comment("virtual column definitions").kind(TableMetadata.Kind.VIRTUAL).addPartitionKeyColumn((String)"keyspace_name", UTF8Type.instance).addClusteringColumn((String)"table_name", UTF8Type.instance).addClusteringColumn((String)"column_name", UTF8Type.instance).addRegularColumn((String)"clustering_order", UTF8Type.instance).addRegularColumn((String)"column_name_bytes", BytesType.instance).addRegularColumn((String)"kind", UTF8Type.instance).addRegularColumn((String)"position", Int32Type.instance).addRegularColumn((String)"type", UTF8Type.instance).build());
      }

      public void addRowData(KeyspaceMetadata keyspace) {
         DataSet data = this.data();
         Iterator var3 = keyspace.tables.iterator();

         while(var3.hasNext()) {
            TableMetadata table = (TableMetadata)var3.next();
            Iterator var5 = table.columns().iterator();

            while(var5.hasNext()) {
               ColumnMetadata column = (ColumnMetadata)var5.next();
               String var10001 = column.ksName;
               DataSet.RowBuilder var10002 = data.newRowBuilder(new Object[]{column.cfName, column.name.toString()}).addColumn("clustering_order", () -> {
                  return column.clusteringOrder().toString().toLowerCase();
               }).addColumn("column_name_bytes", () -> {
                  return column.name.bytes;
               }).addColumn("kind", () -> {
                  return column.kind.toString().toLowerCase();
               });
               column.getClass();
               data.addRow(var10001, var10002.addColumn("position", column::position).addColumn("type", () -> {
                  return column.type.asCQL3Type().toString();
               }));
            }
         }

      }
   }

   private static final class VirtualTables extends InMemoryVirtualTable<KeyspaceMetadata> {
      private static final String KEYSPACE_NAME = "keyspace_name";
      private static final String TABLE_NAME = "table_name";
      private static final String COMMENT = "comment";

      private VirtualTables(String keyspace) {
         super(TableMetadata.builder(keyspace, "tables").comment("virtual table definitions").kind(TableMetadata.Kind.VIRTUAL).addPartitionKeyColumn((String)"keyspace_name", UTF8Type.instance).addClusteringColumn((String)"table_name", UTF8Type.instance).addRegularColumn((String)"comment", UTF8Type.instance).build());
      }

      public void addRowData(KeyspaceMetadata keyspace) {
         DataSet data = this.data();
         Iterator var3 = keyspace.tables.iterator();

         while(var3.hasNext()) {
            TableMetadata table = (TableMetadata)var3.next();
            data.addRow(table.keyspace, data.newRowBuilder(new Object[]{table.name}).addColumn("comment", () -> {
               return table.params.comment;
            })).subscribe();
         }

      }
   }

   private static final class VirtualKeyspaces extends InMemoryVirtualTable<KeyspaceMetadata> {
      private static final String KEYSPACE_NAME = "keyspace_name";

      private VirtualKeyspaces(String keyspace) {
         super(TableMetadata.builder(keyspace, "keyspaces").comment("virtual keyspace definitions").kind(TableMetadata.Kind.VIRTUAL).addPartitionKeyColumn((String)"keyspace_name", UTF8Type.instance).build());
      }

      public void addRowData(KeyspaceMetadata keyspace) {
         this.data().addRow(keyspace.name).subscribe();
      }
   }
}
