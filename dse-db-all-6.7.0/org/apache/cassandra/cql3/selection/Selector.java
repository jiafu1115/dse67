package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class Selector {
   public static final Versioned<ReadVerbs.ReadVersion, Selector.Serializer> serializers = ReadVerbs.ReadVersion.versioned((x$0) -> {
      return new Selector.Serializer(x$0);
   });
   private final Selector.Kind kind;

   public final Selector.Kind kind() {
      return this.kind;
   }

   protected Selector(Selector.Kind kind) {
      this.kind = kind;
   }

   public abstract void addFetchedColumns(ColumnFilter.Builder var1);

   public abstract void addInput(Selector.InputRow var1);

   public abstract ByteBuffer getOutput(ProtocolVersion var1);

   protected Timestamps getWritetimes(ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
   }

   protected Timestamps getTTLs(ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
   }

   public abstract AbstractType<?> getType();

   public abstract void reset();

   public boolean isTerminal() {
      return false;
   }

   public void validateForGroupBy() {
      throw RequestValidations.invalidRequest("Only column names and monotonic scalar functions are supported in the GROUP BY clause.");
   }

   protected abstract int serializedSize(ReadVerbs.ReadVersion var1);

   protected abstract void serialize(DataOutputPlus var1, ReadVerbs.ReadVersion var2) throws IOException;

   protected static void writeType(DataOutputPlus out, AbstractType<?> type) throws IOException {
      out.writeUTF(type.asCQL3Type().toString());
   }

   protected static int sizeOf(AbstractType<?> type) {
      return TypeSizes.sizeof(type.asCQL3Type().toString());
   }

   public static class Serializer extends VersionDependent<ReadVerbs.ReadVersion> {
      private Serializer(ReadVerbs.ReadVersion version) {
         super(version);
      }

      public void serialize(Selector selector, DataOutputPlus out) throws IOException {
         out.writeByte(selector.kind().ordinal());
         selector.serialize(out, (ReadVerbs.ReadVersion)this.version);
      }

      public Selector deserialize(DataInputPlus in, TableMetadata metadata) throws IOException {
         Selector.Kind kind = Selector.Kind.values()[in.readUnsignedByte()];
         return kind.deserializer.deserialize(in, (ReadVerbs.ReadVersion)this.version, metadata);
      }

      public int serializedSize(Selector selector) {
         return TypeSizes.sizeof((byte)selector.kind().ordinal()) + selector.serializedSize((ReadVerbs.ReadVersion)this.version);
      }
   }

   private interface RowTimestamps {
      Selector.RowTimestamps NOOP_ROW_TIMESTAMPS = new Selector.RowTimestamps() {
         public void addNoTimestamp(int index) {
         }

         public void addTimestamp(int index, Cell cell, int nowInSec) {
         }

         public Timestamps get(int index) {
            return Timestamps.NO_TIMESTAMP;
         }

         public void reset() {
         }

         public void setCapacity(int index, int capacity) {
         }
      };

      static Selector.RowTimestamps newInstance(MutableTimestamps.TimestampsType type, List<ColumnMetadata> columns) {
         final MutableTimestamps[] array = new MutableTimestamps[columns.size()];
         int i = 0;

         for(int m = columns.size(); i < m; ++i) {
            array[i] = MutableTimestamps.newTimestamps(type, ((ColumnMetadata)columns.get(i)).type);
         }

         return new Selector.RowTimestamps() {
            public void addNoTimestamp(int index) {
               array[index].addNoTimestamp();
            }

            public void addTimestamp(int index, Cell cell, int nowInSec) {
               array[index].addTimestampFrom(cell, nowInSec);
            }

            public Timestamps get(int index) {
               return array[index];
            }

            public void reset() {
               MutableTimestamps[] var1 = array;
               int var2 = var1.length;

               for(int var3 = 0; var3 < var2; ++var3) {
                  MutableTimestamps timestamps = var1[var3];
                  timestamps.reset();
               }

            }

            public void setCapacity(int index, int capacity) {
               array[index].capacity(capacity);
            }
         };
      }

      void addNoTimestamp(int var1);

      void addTimestamp(int var1, Cell var2, int var3);

      Timestamps get(int var1);

      void reset();

      void setCapacity(int var1, int var2);
   }

   public static final class InputRow {
      private final ProtocolVersion protocolVersion;
      private final List<ColumnMetadata> columns;
      private ByteBuffer[] values;
      private Selector.RowTimestamps timestamps;
      private Selector.RowTimestamps ttls;
      private int index;

      public InputRow(ProtocolVersion protocolVersion, List<ColumnMetadata> columns) {
         this(protocolVersion, columns, false, false);
      }

      public InputRow(ProtocolVersion protocolVersion, List<ColumnMetadata> columns, boolean collectTimestamps, boolean collectTTLs) {
         this.protocolVersion = protocolVersion;
         this.columns = columns;
         this.values = new ByteBuffer[columns.size()];
         this.timestamps = this.initTimestamps(MutableTimestamps.TimestampsType.WRITETIMESTAMPS, collectTimestamps, columns);
         this.ttls = this.initTimestamps(MutableTimestamps.TimestampsType.TTLS, collectTTLs, columns);
      }

      private Selector.RowTimestamps initTimestamps(MutableTimestamps.TimestampsType type, boolean collectTimestamps, List<ColumnMetadata> columns) {
         return collectTimestamps?Selector.RowTimestamps.newInstance(type, columns):Selector.RowTimestamps.NOOP_ROW_TIMESTAMPS;
      }

      public ProtocolVersion getProtocolVersion() {
         return this.protocolVersion;
      }

      public void add(ByteBuffer v) {
         this.values[this.index] = v;
         if(v != null) {
            this.timestamps.addNoTimestamp(this.index);
            this.ttls.addNoTimestamp(this.index);
         }

         ++this.index;
      }

      public void add(ColumnData columnData, int nowInSec) {
         ColumnMetadata column = (ColumnMetadata)this.columns.get(this.index);
         if(columnData == null) {
            this.add((ByteBuffer)null);
         } else if(column.isComplex()) {
            this.add((ComplexColumnData)columnData, nowInSec);
         } else {
            this.add((Cell)columnData, nowInSec);
         }

      }

      private void add(Cell c, int nowInSec) {
         this.values[this.index] = this.value(c);
         this.timestamps.addTimestamp(this.index, c, nowInSec);
         this.ttls.addTimestamp(this.index, c, nowInSec);
         ++this.index;
      }

      private void add(ComplexColumnData ccd, int nowInSec) {
         AbstractType<?> type = ((ColumnMetadata)this.columns.get(this.index)).type;
         if(type.isCollection()) {
            this.values[this.index] = ((CollectionType)type).serializeForNativeProtocol(ccd.iterator(), this.protocolVersion);
            this.timestamps.setCapacity(this.index, ccd.cellsCount());
            this.ttls.setCapacity(this.index, ccd.cellsCount());
            Iterator var4 = ccd.iterator();

            while(var4.hasNext()) {
               Cell cell = (Cell)var4.next();
               this.timestamps.addTimestamp(this.index, cell, nowInSec);
               this.ttls.addTimestamp(this.index, cell, nowInSec);
            }
         } else {
            UserType udt = (UserType)type;
            int size = udt.size();
            this.values[this.index] = udt.serializeForNativeProtocol(ccd.iterator(), this.protocolVersion);
            short fieldPosition = 0;
            Iterator var7 = ccd.iterator();

            while(var7.hasNext()) {
               Cell cell = (Cell)var7.next();
               short fieldPositionOfCell = ByteBufferUtil.toShort(cell.path().get(0));

               while(fieldPosition < fieldPositionOfCell) {
                  ++fieldPosition;
                  this.timestamps.addNoTimestamp(this.index);
                  this.ttls.addNoTimestamp(this.index);
               }

               ++fieldPosition;
               this.timestamps.addTimestamp(this.index, cell, nowInSec);
               this.ttls.addTimestamp(this.index, cell, nowInSec);
            }

            while(fieldPosition < size) {
               ++fieldPosition;
               this.timestamps.addNoTimestamp(this.index);
               this.ttls.addNoTimestamp(this.index);
            }
         }

         ++this.index;
      }

      private ByteBuffer value(Cell c) {
         return c.isCounterCell()?ByteBufferUtil.bytes(CounterContext.instance().total(c.value())):c.value();
      }

      public ByteBuffer getValue(int index) {
         return this.values[index];
      }

      public void reset(boolean deep) {
         this.index = 0;
         this.timestamps.reset();
         this.ttls.reset();
         if(deep) {
            this.values = new ByteBuffer[this.values.length];
         }

      }

      Timestamps getTimestamps(int columnIndex) {
         return this.timestamps.get(columnIndex);
      }

      Timestamps getTtls(int columnIndex) {
         return this.ttls.get(columnIndex);
      }

      public List<ByteBuffer> getValues() {
         return Arrays.asList(this.values);
      }
   }

   public abstract static class Factory {
      public Factory() {
      }

      public void addFunctionsTo(List<org.apache.cassandra.cql3.functions.Function> functions) {
      }

      public ColumnSpecification getColumnSpecification(TableMetadata table) {
         return new ColumnSpecification(table.keyspace, table.name, new ColumnIdentifier(this.getColumnName(), true), this.getReturnType());
      }

      public abstract Selector newInstance(QueryOptions var1);

      public boolean isAggregateSelectorFactory() {
         return false;
      }

      public boolean isWritetimeSelectorFactory() {
         return false;
      }

      public boolean isTTLSelectorFactory() {
         return false;
      }

      public boolean isSimpleSelectorFactory() {
         return false;
      }

      public boolean isSimpleSelectorFactoryFor(int index) {
         return false;
      }

      protected abstract String getColumnName();

      protected abstract AbstractType<?> getReturnType();

      protected abstract void addColumnMapping(SelectionColumnMapping var1, ColumnSpecification var2);

      abstract boolean areAllFetchedColumnsKnown();

      abstract void addFetchedColumns(ColumnFilter.Builder var1);
   }

   public static enum Kind {
      SIMPLE_SELECTOR(SimpleSelector.deserializer),
      TERM_SELECTOR(TermSelector.deserializer),
      WRITETIME_OR_TTL_SELECTOR(WritetimeOrTTLSelector.deserializer),
      LIST_SELECTOR(ListSelector.deserializer),
      SET_SELECTOR(SetSelector.deserializer),
      MAP_SELECTOR(MapSelector.deserializer),
      TUPLE_SELECTOR(TupleSelector.deserializer),
      USER_TYPE_SELECTOR(UserTypeSelector.deserializer),
      FIELD_SELECTOR(FieldSelector.deserializer),
      SCALAR_FUNCTION_SELECTOR(ScalarFunctionSelector.deserializer),
      AGGREGATE_FUNCTION_SELECTOR(AggregateFunctionSelector.deserializer),
      ELEMENT_SELECTOR(ElementsSelector.ElementSelector.deserializer),
      SLICE_SELECTOR(ElementsSelector.SliceSelector.deserializer);

      private final Selector.SelectorDeserializer deserializer;

      private Kind(Selector.SelectorDeserializer deserializer) {
         this.deserializer = deserializer;
      }
   }

   protected abstract static class SelectorDeserializer {
      protected SelectorDeserializer() {
      }

      protected abstract Selector deserialize(DataInputPlus var1, ReadVerbs.ReadVersion var2, TableMetadata var3) throws IOException;

      protected final AbstractType<?> readType(TableMetadata metadata, DataInputPlus in) throws IOException {
         KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(metadata.keyspace);
         return this.readType(keyspace, in);
      }

      protected final AbstractType<?> readType(KeyspaceMetadata keyspace, DataInputPlus in) throws IOException {
         String cqlType = in.readUTF();
         return CQLTypeParser.parse(keyspace.name, cqlType, keyspace.types);
      }
   }
}
