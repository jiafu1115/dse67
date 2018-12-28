package org.apache.cassandra.cql3.restrictions;

import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;

public interface Restriction {
   default boolean isOnToken() {
      return false;
   }

   ColumnMetadata getFirstColumn();

   ColumnMetadata getLastColumn();

   List<ColumnMetadata> getColumnDefs();

   void addFunctionsTo(List<Function> var1);

   void forEachFunction(Consumer<Function> var1);

   boolean hasSupportingIndex(IndexRegistry var1);

   void addRowFilterTo(RowFilter var1, IndexRegistry var2, QueryOptions var3);
}
