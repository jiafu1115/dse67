package org.apache.cassandra.utils;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

public class SortedBiMultiValMap<K, V> extends BiMultiValMap<K, V> {
   private static final Comparator DEFAULT_COMPARATOR = (o1, o2) -> {
      return ((Comparable)o1).compareTo(o2);
   };

   protected SortedBiMultiValMap(SortedMap<K, V> forwardMap, SortedSetMultimap<V, K> reverseMap) {
      super(forwardMap, reverseMap);
   }

   public static <K extends Comparable<K>, V extends Comparable<V>> SortedBiMultiValMap<K, V> create() {
      return new SortedBiMultiValMap(new TreeMap(), TreeMultimap.create());
   }

   public static <K, V> SortedBiMultiValMap<K, V> create(Comparator<K> keyComparator, Comparator<V> valueComparator) {
      if(keyComparator == null) {
         keyComparator = defaultComparator();
      }

      if(valueComparator == null) {
         valueComparator = defaultComparator();
      }

      return new SortedBiMultiValMap(new TreeMap(keyComparator), TreeMultimap.create(valueComparator, keyComparator));
   }

   public static <K extends Comparable<K>, V extends Comparable<V>> SortedBiMultiValMap<K, V> create(BiMultiValMap<K, V> map) {
      SortedBiMultiValMap<K, V> newMap = create();
      copy(map, newMap);
      return newMap;
   }

   public static <K, V> SortedBiMultiValMap<K, V> create(BiMultiValMap<K, V> map, Comparator<K> keyComparator, Comparator<V> valueComparator) {
      SortedBiMultiValMap<K, V> newMap = create(keyComparator, valueComparator);
      copy(map, newMap);
      return newMap;
   }

   private static <K, V> void copy(BiMultiValMap<K, V> map, BiMultiValMap<K, V> newMap) {
      newMap.forwardMap.putAll(map.forwardMap);
      Iterator var2 = map.inverse().asMap().entrySet().iterator();

      while(var2.hasNext()) {
         Entry<V, Collection<K>> entry = (Entry)var2.next();
         newMap.reverseMap.putAll(entry.getKey(), (Iterable)entry.getValue());
      }

   }

   private static <T> Comparator<T> defaultComparator() {
      return DEFAULT_COMPARATOR;
   }
}
