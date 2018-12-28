package org.apache.cassandra.schema;

import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;

public interface SchemaChangeListener {
   default void onCreateKeyspace(String keyspace) {
   }

   default void onCreateTable(String keyspace, String table) {
   }

   default void onCreateView(String keyspace, String view) {
      this.onCreateTable(keyspace, view);
   }

   default void onCreateType(String keyspace, String type) {
   }

   default void onCreateFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
   }

   default void onCreateAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
   }

   default void onAlterKeyspace(String keyspace) {
   }

   default void onAlterTable(String keyspace, String table, boolean affectsStatements) {
   }

   default void onAlterView(String keyspace, String view, boolean affectsStataments) {
      this.onAlterTable(keyspace, view, affectsStataments);
   }

   default void onAlterType(String keyspace, String type) {
   }

   default void onAlterFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
   }

   default void onAlterAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
   }

   default void onDropKeyspace(String keyspace) {
   }

   default void onDropTable(String keyspace, String table) {
   }

   default void onDropView(String keyspace, String view) {
      this.onDropTable(keyspace, view);
   }

   default void onDropType(String keyspace, String type) {
   }

   default void onDropFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
   }

   default void onDropAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
   }

   default void onCreateVirtualKeyspace(String keyspace) {
   }

   default void onCreateVirtualTable(String keyspace, String table) {
   }
}
