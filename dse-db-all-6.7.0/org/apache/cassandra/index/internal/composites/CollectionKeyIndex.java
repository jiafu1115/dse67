package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;

public class CollectionKeyIndex extends CollectionKeyIndexBase {
   public CollectionKeyIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      super(baseCfs, indexDef);
   }

   public ByteBuffer getIndexedValue(ByteBuffer partitionKey, Clustering clustering, CellPath path, ByteBuffer cellValue) {
      return path.get(0);
   }

   public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec) {
      Cell cell = data.getCell(this.indexedColumn, CellPath.create(indexValue));
      return cell == null || !cell.isLive(nowInSec);
   }

   public boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator) {
      return operator == Operator.CONTAINS_KEY || operator == Operator.CONTAINS && indexedColumn.type instanceof SetType;
   }
}
