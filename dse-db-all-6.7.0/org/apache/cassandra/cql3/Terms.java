package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public interface Terms {
   List UNSET_LIST = new AbstractList() {
      public Object get(int index) {
         throw new UnsupportedOperationException();
      }

      public int size() {
         return 0;
      }
   };

   void addFunctionsTo(List<Function> var1);

   void forEachFunction(Consumer<Function> var1);

   void collectMarkerSpecification(VariableSpecifications var1);

   List<Term.Terminal> bind(QueryOptions var1);

   List<ByteBuffer> bindAndGet(QueryOptions var1);

   static default Terms ofListMarker(final Lists.Marker marker, final AbstractType<?> type) {
      return new Terms() {
         public void addFunctionsTo(List<Function> functions) {
         }

         public void forEachFunction(Consumer<Function> consumer) {
         }

         public void collectMarkerSpecification(VariableSpecifications boundNames) {
            marker.collectMarkerSpecification(boundNames);
         }

         public List<ByteBuffer> bindAndGet(QueryOptions options) {
            Term.Terminal terminal = marker.bind(options);
            return terminal == null?null:(terminal == Constants.UNSET_VALUE?UNSET_LIST:((Term.MultiItemTerminal)terminal).getElements());
         }

         public List<Term.Terminal> bind(QueryOptions options) {
            Term.Terminal terminal = marker.bind(options);
            if(terminal == null) {
               return null;
            } else if(terminal == Constants.UNSET_VALUE) {
               return UNSET_LIST;
            } else {
               java.util.function.Function<ByteBuffer, Term.Terminal> deserializer = this.deserializer(options.getProtocolVersion());
               List<ByteBuffer> boundValues = ((Term.MultiItemTerminal)terminal).getElements();
               List<Term.Terminal> values = new ArrayList(boundValues.size());
               int i = 0;

               for(int m = boundValues.size(); i < m; ++i) {
                  ByteBuffer buffer = (ByteBuffer)boundValues.get(i);
                  Term.Terminal value = buffer == null?null:(Term.Terminal)deserializer.apply(buffer);
                  values.add(value);
               }

               return values;
            }
         }

         public java.util.function.Function<ByteBuffer, Term.Terminal> deserializer(ProtocolVersion version) {
            if(type.isCollection()) {
               switch(null.$SwitchMap$org$apache$cassandra$db$marshal$CollectionType$Kind[((CollectionType)type).kind.ordinal()]) {
               case 1:
                  return (e) -> {
                     return Lists.Value.fromSerialized(e, (ListType)type, version);
                  };
               case 2:
                  return (e) -> {
                     return Sets.Value.fromSerialized(e, (SetType)type, version);
                  };
               case 3:
                  return (e) -> {
                     return Maps.Value.fromSerialized(e, (MapType)type, version);
                  };
               default:
                  throw new AssertionError();
               }
            } else {
               return (e) -> {
                  return new Constants.Value(e);
               };
            }
         }
      };
   }

   static default Terms of(final Term term) {
      return new Terms() {
         public void addFunctionsTo(List<Function> functions) {
            term.addFunctionsTo(functions);
         }

         public void forEachFunction(Consumer<Function> consumer) {
            term.forEachFunction(consumer);
         }

         public void collectMarkerSpecification(VariableSpecifications boundNames) {
            term.collectMarkerSpecification(boundNames);
         }

         public List<ByteBuffer> bindAndGet(QueryOptions options) {
            return UnmodifiableArrayList.of((Object)term.bindAndGet(options));
         }

         public List<Term.Terminal> bind(QueryOptions options) {
            return UnmodifiableArrayList.of((Object)term.bind(options));
         }
      };
   }

   static default Terms of(final List<Term> terms) {
      return new Terms() {
         public void addFunctionsTo(List<Function> functions) {
            Terms.addFunctions(terms, functions);
         }

         public void forEachFunction(Consumer<Function> consumer) {
            Terms.forEachFunction(terms, consumer);
         }

         public void collectMarkerSpecification(VariableSpecifications boundNames) {
            int i = 0;

            for(int m = terms.size(); i < m; ++i) {
               Term term = (Term)terms.get(i);
               term.collectMarkerSpecification(boundNames);
            }

         }

         public List<Term.Terminal> bind(QueryOptions options) {
            int size = terms.size();
            List<Term.Terminal> terminals = new ArrayList(size);

            for(int i = 0; i < size; ++i) {
               Term term = (Term)terms.get(i);
               terminals.add(term.bind(options));
            }

            return terminals;
         }

         public List<ByteBuffer> bindAndGet(QueryOptions options) {
            int size = terms.size();
            List<ByteBuffer> buffers = new ArrayList(size);

            for(int i = 0; i < size; ++i) {
               Term term = (Term)terms.get(i);
               buffers.add(term.bindAndGet(options));
            }

            return buffers;
         }
      };
   }

   static default void addFunctions(Iterable<Term> terms, List<Function> functions) {
      Iterator var2 = terms.iterator();

      while(var2.hasNext()) {
         Term term = (Term)var2.next();
         if(term != null) {
            term.addFunctionsTo(functions);
         }
      }

   }

   static default void forEachFunction(List<Term> terms, Consumer<Function> consumer) {
      for(int i = 0; i < terms.size(); ++i) {
         Term term = (Term)terms.get(i);
         if(term != null) {
            term.forEachFunction(consumer);
         }
      }

   }

   static default void forEachFunction(Set<Term> terms, Consumer<Function> consumer) {
      Iterator var2 = terms.iterator();

      while(var2.hasNext()) {
         Term term = (Term)var2.next();
         if(term != null) {
            term.forEachFunction(consumer);
         }
      }

   }

   static default void forEachFunction(Map<? extends Term, ? extends Term> terms, Consumer<Function> consumer) {
      terms.forEach((k, v) -> {
         if(k != null) {
            k.forEachFunction(consumer);
         }

         if(v != null) {
            v.forEachFunction(consumer);
         }

      });
   }

   static default ByteBuffer asBytes(String keyspace, String term, AbstractType type) {
      ColumnSpecification receiver = new ColumnSpecification(keyspace, "--dummy--", new ColumnIdentifier("(dummy)", true), type);
      Term.Raw rawTerm = (Term.Raw)CQLFragmentParser.parseAny(CqlParser::term, term, "CQL term");
      return rawTerm.prepare(keyspace, receiver).bindAndGet(QueryOptions.DEFAULT);
   }
}
