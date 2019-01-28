package org.apache.cassandra.index;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

public interface IndexRegistry {
   IndexRegistry EMPTY = new IndexRegistry() {
      public void unregisterIndex(Index index) {
      }

      public void registerIndex(Index index) {
      }

      public Collection<Index> listIndexes() {
         return Collections.emptyList();
      }

      public Index getIndex(IndexMetadata indexMetadata) {
         return null;
      }

      public Optional<Index> getBestIndexFor(RowFilter.Expression expression) {
         return Optional.empty();
      }

      public void validate(PartitionUpdate update) {
      }
   };

   void registerIndex(Index var1);

   void unregisterIndex(Index var1);

   Index getIndex(IndexMetadata var1);

   Collection<Index> listIndexes();

   Optional<Index> getBestIndexFor(RowFilter.Expression var1);

   void validate(PartitionUpdate var1);

   static IndexRegistry obtain(TableMetadata table) {
      return (IndexRegistry)(table.isVirtual()?EMPTY:Keyspace.openAndGetStore(table).indexManager);
   }
}
