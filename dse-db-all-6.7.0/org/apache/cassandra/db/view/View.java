package org.apache.cassandra.db.view;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class View {
   private static final Logger logger = LoggerFactory.getLogger(View.class);
   public final String name;
   private volatile ViewMetadata definition;
   private final ColumnFamilyStore baseCfs;
   private ViewBuilder builder;
   private final SelectStatement.RawStatement rawSelect;
   private SelectStatement select;
   private ReadQuery query;

   public View(ViewMetadata definition, ColumnFamilyStore baseCfs) {
      this.baseCfs = baseCfs;
      this.name = definition.name;
      this.rawSelect = definition.select;
      this.definition = definition;
      logger.info("Initializing Materialized View {} on base table {}.", String.format("%s%s", new Object[]{definition.name, definition.isLegacyView()?"(legacy)":""}), baseCfs.name);
      if(definition.isLegacyView()) {
         if(definition.hasSamePrimaryKeyColumnsAsBaseTable() && definition.hasUnselectedColumns()) {
            logger.warn("Legacy view {}.{} detected with unselected columns. Out of order updates to these columns may not be reflected on view. Re-create view to remove this limitation.", baseCfs.keyspace, this.name);
         }

         if(definition.hasFilteredNonPrimaryKeyColumns()) {
            logger.warn("Legacy view {}.{} detected with filtered columns. Expiration or removal of filtered columns may not remove view rows in some scenarios. Re-create view to remove this limitation.", baseCfs.keyspace, this.name);
         }
      }

   }

   public ViewMetadata getDefinition() {
      return this.definition;
   }

   public void updateDefinition(ViewMetadata definition) {
      this.definition = definition;
   }

   public List<RowFilter.Expression> getFilteredExpressions(ColumnMetadata base) {
      return (List)this.getSelectStatement().rowFilterForInternalCalls().getExpressions().stream().filter((e) -> {
         return e.column().equals(base);
      }).collect(Collectors.toList());
   }

   public boolean mayBeAffectedBy(DecoratedKey partitionKey, Row update) {
      return this.getReadQuery().selectsClustering(partitionKey, update.clustering());
   }

   public Flow<Boolean> matchesViewFilter(DecoratedKey partitionKey, Row baseRow, int nowInSec) {
      return !this.getReadQuery().selectsClustering(partitionKey, baseRow.clustering())?Flow.just(Boolean.valueOf(false)):this.getSelectStatement().rowFilterForInternalCalls().isSatisfiedBy(this.definition.baseTableMetadata, partitionKey, baseRow, nowInSec);
   }

   public SelectStatement getSelectStatement() {
      if(this.select == null) {
         ClientState state = ClientState.forInternalCalls();
         state.setKeyspace(this.baseCfs.keyspace.getName());
         this.rawSelect.prepareKeyspace(state);
         ParsedStatement.Prepared prepared = this.rawSelect.prepare(true);
         this.select = (SelectStatement)prepared.statement;
      }

      return this.select;
   }

   public ReadQuery getReadQuery() {
      if(this.query == null) {
         this.query = this.getSelectStatement().getQuery(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(UnmodifiableArrayList.emptyList()), ApolloTime.systemClockSecondsAsInt());
         logger.trace("View query: {}", this.rawSelect);
      }

      return this.query;
   }

   public synchronized void build() {
      this.stopBuild();
      this.builder = new ViewBuilder(this.baseCfs, this);
      this.builder.start();
   }

   synchronized void stopBuild() {
      if(this.builder != null) {
         logger.debug("Stopping current view builder due to schema change");
         this.builder.stop();
         this.builder = null;
      }

   }

   @Nullable
   public static TableMetadataRef findBaseTable(String keyspace, String viewName) {
      ViewMetadata view = Schema.instance.getView(keyspace, viewName);
      return view == null?null:Schema.instance.getTableMetadataRef(view.baseTableId());
   }

   public static Iterable<ViewMetadata> findAll(String keyspace, String baseTable) {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
      return Iterables.filter(ksm.views, (view) -> {
         return view.baseTableName().equals(baseTable);
      });
   }
}
