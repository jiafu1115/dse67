package org.apache.cassandra.db.virtual;

import io.reactivex.Completable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;

public final class VirtualMutation implements IMutation {
   private final String keyspaceName;
   private final DecoratedKey partitionKey;
   private final Map<TableId, PartitionUpdate> modifications;

   public VirtualMutation(String keyspaceName, DecoratedKey partitionKey) {
      this(keyspaceName, partitionKey, new HashMap(4));
   }

   public VirtualMutation(PartitionUpdate update) {
      this(update.metadata().keyspace, update.partitionKey(), Collections.singletonMap(update.metadata().id, update));
   }

   private VirtualMutation(String keyspaceName, DecoratedKey partitionKey, Map<TableId, PartitionUpdate> modifications) {
      this.keyspaceName = keyspaceName;
      this.partitionKey = partitionKey;
      this.modifications = modifications;
   }

   public void apply() {
      TPCUtils.blockingAwait(this.applyAsync());
   }

   public String getKeyspaceName() {
      return this.keyspaceName;
   }

   public Collection<TableId> getTableIds() {
      return this.modifications.keySet();
   }

   public DecoratedKey key() {
      return this.partitionKey;
   }

   public long getTimeout() {
      return DatabaseDescriptor.getWriteRpcTimeout();
   }

   public Collection<PartitionUpdate> getPartitionUpdates() {
      return this.modifications.values();
   }

   public PartitionUpdate get(TableMetadata metadata) {
      return (PartitionUpdate)this.modifications.get(metadata.id);
   }

   public Completable applyAsync() {
      List<Completable> completables = new ArrayList(this.modifications.size());
      Iterator var2 = this.modifications.values().iterator();

      while(var2.hasNext()) {
         PartitionUpdate update = (PartitionUpdate)var2.next();
         Schema.instance.getVirtualTableInstance(update.metadata().id).apply(update);
      }

      return Completable.concat(completables);
   }

   public String toString(boolean shallow) {
      StringBuilder buff = new StringBuilder("VirtualMutation(");
      buff.append("keyspace='").append(this.keyspaceName).append('\'');
      buff.append(", key='").append(ByteBufferUtil.bytesToHex(this.key().getKey())).append('\'');
      buff.append(", modifications=[");
      if(shallow) {
         List<String> tableNames = new ArrayList(this.modifications.size());
         Iterator var4 = this.modifications.keySet().iterator();

         while(var4.hasNext()) {
            TableId tableId = (TableId)var4.next();
            TableMetadata table = Schema.instance.getTableMetadata(tableId);
            tableNames.add(table == null?"-dropped-":table.name);
         }

         buff.append(StringUtils.join(tableNames, ", "));
      } else {
         buff.append("\n  ").append(StringUtils.join(this.modifications.values(), "\n  ")).append('\n');
      }

      return buff.append("])").toString();
   }

   public VirtualMutation add(PartitionUpdate update) {
      PartitionUpdate prev = (PartitionUpdate)this.modifications.put(update.metadata().id, update);
      if(prev != null) {
         throw new IllegalArgumentException("Table " + update.metadata().name + " already has modifications in this mutation: " + prev);
      } else {
         return this;
      }
   }
}
