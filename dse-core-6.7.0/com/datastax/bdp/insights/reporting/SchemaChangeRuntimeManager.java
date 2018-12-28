package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.datastax.bdp.insights.events.SchemaChangeInformation;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SchemaChangeRuntimeManager implements InsightsRuntimeConfigComponent, SchemaChangeListener {
   private static final Logger logger = LoggerFactory.getLogger(SchemaChangeRuntimeManager.class);
   private final InsightsClient client;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public SchemaChangeRuntimeManager(InsightsClient client) {
      this.client = client;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         Schema.instance.registerListener(this);
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         Schema.instance.unregisterListener(this);
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.schema_change");
   }

   public void onCreateKeyspace(String keyspace) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_KEYSPACE, keyspace));
      } catch (Throwable var3) {
         logger.warn("Exception notifying schema change", var3);
      }

   }

   public void onCreateTable(String keyspace, String table) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_TABLE, keyspace, table));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onCreateView(String keyspace, String view) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_VIEW, keyspace, view));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onCreateType(String keyspace, String type) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_TYPE, keyspace, type));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onCreateFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_FUNCTION, keyspace, function));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onCreateAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_AGGREGATE, keyspace, aggregate));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onAlterKeyspace(String keyspace) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.ALTER_KEYSPACE, keyspace));
      } catch (Throwable var3) {
         logger.warn("Exception notifying schema change", var3);
      }

   }

   public void onAlterTable(String keyspace, String table, boolean affectsStatements) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.ALTER_TABLE, keyspace, table));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onAlterView(String keyspace, String view, boolean affectsStataments) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.ALTER_VIEW, keyspace, view));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onAlterType(String keyspace, String type) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.ALTER_TYPE, keyspace, type));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onAlterFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.ALTER_FUNCTION, keyspace, function));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onAlterAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.ALTER_AGGREGATE, keyspace, aggregate));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onDropKeyspace(String keyspace) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.DROP_KEYSPACE, keyspace));
      } catch (Throwable var3) {
         logger.warn("Exception notifying schema change", var3);
      }

   }

   public void onDropTable(String keyspace, String table) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.DROP_TABLE, keyspace, table));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onDropView(String keyspace, String view) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.DROP_VIEW, keyspace, view));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onDropType(String keyspace, String type) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.DROP_TYPE, keyspace, type));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }

   public void onDropFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.DROP_FUNCTION, keyspace, function));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onDropAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.DROP_AGGREGATE, keyspace, aggregate));
      } catch (Throwable var5) {
         logger.warn("Exception notifying schema change", var5);
      }

   }

   public void onCreateVirtualKeyspace(String keyspace) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_VIRTUAL_KEYSPACE, keyspace));
      } catch (Throwable var3) {
         logger.warn("Exception notifying schema change", var3);
      }

   }

   public void onCreateVirtualTable(String keyspace, String table) {
      try {
         this.client.report(new SchemaChangeInformation(SchemaChangeInformation.ChangeType.CREATE_VIRTUAL_TABLE, keyspace, table));
      } catch (Throwable var4) {
         logger.warn("Exception notifying schema change", var4);
      }

   }
}
