package org.apache.cassandra.db.virtual;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public class VirtualKeyspace {
   private final String name;
   private final KeyspaceMetadata metadata;
   private final UnmodifiableArrayList<VirtualTable> tables;

   public VirtualKeyspace(String name, Collection<VirtualTable> tables) {
      this.name = name;
      this.tables = UnmodifiableArrayList.copyOf(tables);
      this.metadata = KeyspaceMetadata.virtual(name, Tables.of(Iterables.transform(tables, VirtualTable::metadata)));
   }

   public String name() {
      return this.name;
   }

   public KeyspaceMetadata metadata() {
      return this.metadata;
   }

   public UnmodifiableArrayList<VirtualTable> tables() {
      return this.tables;
   }
}
