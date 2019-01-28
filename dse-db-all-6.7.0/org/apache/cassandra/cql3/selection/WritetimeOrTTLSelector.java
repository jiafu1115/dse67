package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

final class WritetimeOrTTLSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         Selector selected = ((Selector.Serializer)Selector.serializers.get(version)).deserialize(in, metadata);
         int columnIndex = in.readInt();
         boolean isWritetime = in.readBoolean();
         boolean isMultiCell = in.readBoolean();
         return new WritetimeOrTTLSelector(selected, columnIndex, isWritetime, isMultiCell);
      }
   };
   private final Selector selected;
   private final int columnIndex;
   private final boolean isWritetime;
   private final boolean isMultiCell;
   private ByteBuffer output;
   private boolean isSet;

   public static Selector.Factory newFactory(final Selector.Factory factory, final int columnIndex, final boolean isWritetime, final boolean isMultiCell) {
      return new Selector.Factory() {
         protected String getColumnName() {
            return String.format("%s(%s)", new Object[]{isWritetime?"writetime":"ttl", factory.getColumnName()});
         }

         protected AbstractType<?> getReturnType() {
            AbstractType<?> type = isWritetime?LongType.instance:Int32Type.instance;
            return (AbstractType)(isMultiCell?ListType.getInstance((AbstractType)type, false):type);
         }

         protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn) {
            factory.addColumnMapping(mapping, resultsColumn);
         }

         public Selector newInstance(QueryOptions options) {
            return new WritetimeOrTTLSelector(factory.newInstance(options), columnIndex, isWritetime, isMultiCell);
         }

         public boolean isWritetimeSelectorFactory() {
            return isWritetime;
         }

         public boolean isTTLSelectorFactory() {
            return !isWritetime;
         }

         public boolean areAllFetchedColumnsKnown() {
            return true;
         }

         public void addFetchedColumns(ColumnFilter.Builder builder) {
            factory.addFetchedColumns(builder);
         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      this.selected.addFetchedColumns(builder);
   }

   public void addInput(Selector.InputRow input) {
      if(!this.isSet) {
         this.isSet = true;
         this.selected.addInput(input);
         ProtocolVersion protocolVersion = input.getProtocolVersion();
         this.output = this.isWritetime?this.selected.getWritetimes(protocolVersion).toByteBuffer(protocolVersion):this.selected.getTTLs(protocolVersion).toByteBuffer(protocolVersion);
      }
   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      return this.output;
   }

   public void reset() {
      this.selected.reset();
      this.isSet = false;
      this.output = null;
   }

   public AbstractType<?> getType() {
      AbstractType<?> type = this.isWritetime?LongType.instance:Int32Type.instance;
      return (AbstractType)(this.isMultiCell?ListType.getInstance((AbstractType)type, false):type);
   }

   public String toString() {
      return this.selected.toString();
   }

   private WritetimeOrTTLSelector(Selector selected, int columnIndex, boolean isWritetime, boolean isMultiCell) {
      super(Selector.Kind.WRITETIME_OR_TTL_SELECTOR);
      this.selected = selected;
      this.columnIndex = columnIndex;
      this.isWritetime = isWritetime;
      this.isMultiCell = isMultiCell;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof WritetimeOrTTLSelector)) {
         return false;
      } else {
         WritetimeOrTTLSelector s = (WritetimeOrTTLSelector)o;
         return Objects.equals(this.selected, s.selected) && Objects.equals(Boolean.valueOf(this.isWritetime), Boolean.valueOf(s.isWritetime));
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.selected, Boolean.valueOf(this.isWritetime)});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      return ((Selector.Serializer)serializers.get(version)).serializedSize(this.selected) + TypeSizes.sizeof(this.columnIndex) + TypeSizes.sizeof(this.isWritetime);
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      ((Selector.Serializer)serializers.get(version)).serialize(this.selected, out);
      out.writeInt(this.columnIndex);
      out.writeBoolean(this.isWritetime);
      out.writeBoolean(this.isMultiCell);
   }
}
