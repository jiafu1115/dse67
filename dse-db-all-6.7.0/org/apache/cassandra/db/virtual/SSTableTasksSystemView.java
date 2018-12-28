package org.apache.cassandra.db.virtual;

import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.schema.TableMetadata;

public final class SSTableTasksSystemView extends AbstractVirtualTable {
   private static final String KEYSPACE_NAME = "keyspace_name";
   private static final String TABLE_NAME = "table_name";
   private static final String TASK_ID = "task_id";
   private static final String KIND = "kind";
   private static final String PROGRESS = "progress";
   private static final String TOTAL = "total";
   private static final String UNIT = "unit";
   public static final VirtualTable INSTANCE = new SSTableTasksSystemView();

   private SSTableTasksSystemView() {
      super(TableMetadata.builder("system_views", "sstable_tasks").comment("current sstable tasks").kind(TableMetadata.Kind.VIRTUAL).addPartitionKeyColumn((String)"keyspace_name", UTF8Type.instance).addClusteringColumn((String)"table_name", UTF8Type.instance).addClusteringColumn((String)"task_id", UUIDType.instance).addRegularColumn((String)"kind", UTF8Type.instance).addRegularColumn((String)"progress", LongType.instance).addRegularColumn((String)"total", LongType.instance).addRegularColumn((String)"unit", UTF8Type.instance).build());
   }

   public DataSet data() {
      DataSet dataset = this.newDataSet();
      Iterator var2 = CompactionMetrics.getCompactions().iterator();

      while(var2.hasNext()) {
         CompactionInfo.Holder holder = (CompactionInfo.Holder)var2.next();
         CompactionInfo task = holder.getCompactionInfo();
         if(!task.getTaskType().isCacheSave()) {
            Object var10001 = task.getKeyspace().orElse("*");
            DataSet.RowBuilder var10002 = dataset.newRowBuilder(new Object[]{task.getTable().orElse("*"), task.getTaskId()}).addColumn("kind", () -> {
               return task.getTaskType().toString().toLowerCase();
            });
            task.getClass();
            var10002 = var10002.addColumn("progress", task::getCompleted);
            task.getClass();
            dataset.addRow(var10001, var10002.addColumn("total", task::getTotal).addColumn("unit", () -> {
               return task.getUnit().toString().toLowerCase();
            })).subscribe();
         }
      }

      return dataset;
   }
}
