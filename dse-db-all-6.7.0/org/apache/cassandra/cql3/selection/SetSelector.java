package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

final class SetSelector extends Selector {
   protected static final Selector.SelectorDeserializer deserializer = new Selector.SelectorDeserializer() {
      protected Selector deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, TableMetadata metadata) throws IOException {
         SetType<?> type = (SetType)this.readType(metadata, in);
         int size = (int)in.readUnsignedVInt();
         List<Selector> elements = new ArrayList(size);
         Selector.Serializer serializer = (Selector.Serializer)Selector.serializers.get(version);

         for(int i = 0; i < size; ++i) {
            elements.add(serializer.deserialize(in, metadata));
         }

         return new SetSelector(type, elements);
      }
   };
   private final SetType<?> type;
   private final List<Selector> elements;

   public static Selector.Factory newFactory(final AbstractType<?> type, final SelectorFactories factories) {
      return new CollectionFactory(type, factories) {
         protected String getColumnName() {
            return Sets.setToString(factories, Selector.Factory::getColumnName);
         }

         public Selector newInstance(QueryOptions options) {
            return new SetSelector(type, factories.newInstances(options));
         }
      };
   }

   public void addFetchedColumns(ColumnFilter.Builder builder) {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         ((Selector)this.elements.get(i)).addFetchedColumns(builder);
      }

   }

   public void addInput(Selector.InputRow input) {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         ((Selector)this.elements.get(i)).addInput(input);
      }

   }

   public ByteBuffer getOutput(ProtocolVersion protocolVersion) {
      Set<ByteBuffer> buffers = new TreeSet(this.type.getElementsType());
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         buffers.add(((Selector)this.elements.get(i)).getOutput(protocolVersion));
      }

      return CollectionSerializer.pack(buffers, buffers.size(), protocolVersion);
   }

   public boolean isTerminal() {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         if(!((Selector)this.elements.get(i)).isTerminal()) {
            return false;
         }
      }

      return true;
   }

   public void reset() {
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         ((Selector)this.elements.get(i)).reset();
      }

   }

   public AbstractType<?> getType() {
      return this.type;
   }

   public String toString() {
      return Sets.setToString(this.elements);
   }

   private SetSelector(AbstractType<?> type, List<Selector> elements) {
      super(Selector.Kind.SET_SELECTOR);
      this.type = (SetType)type;
      this.elements = elements;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof SetSelector)) {
         return false;
      } else {
         SetSelector s = (SetSelector)o;
         return Objects.equals(this.type, s.type) && Objects.equals(this.elements, s.elements);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.type, this.elements});
   }

   protected int serializedSize(ReadVerbs.ReadVersion version) {
      int size = sizeOf(this.type) + TypeSizes.sizeofUnsignedVInt((long)this.elements.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         size += serializer.serializedSize((Selector)this.elements.get(i));
      }

      return size;
   }

   protected void serialize(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      writeType(out, this.type);
      out.writeUnsignedVInt((long)this.elements.size());
      Selector.Serializer serializer = (Selector.Serializer)serializers.get(version);
      int i = 0;

      for(int m = this.elements.size(); i < m; ++i) {
         serializer.serialize((Selector)this.elements.get(i), out);
      }

   }
}
