package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.config.PropertyConfiguration;

public class SetsFactory {
   private static final boolean USE_JDK_SETS = PropertyConfiguration.getBoolean("dse.use_jdk_sets");
   private static final float LOAD_FACTOR = 0.55F;
   private static final int JDK_DEFAULT_CAPACITY = 16;
   private static final float JDK_DEFAULT_LOAD_FACTOR = 0.75F;

   public SetsFactory() {
   }

   public static <T> Set<T> newSetOfCapacity(int capacity) {
      return (Set)(USE_JDK_SETS?new HashSet(capacity):new SetsFactory.CorrectlySerializingSet(capacity));
   }

   public static <T> Set<T> newSetForSize(int size) {
      return newSetOfCapacity(sizeToCapacity(size));
   }

   private static int sizeToCapacity(int size) {
      return USE_JDK_SETS?sizeToCapacityJDK(size):sizeToCapacityAgrona(size);
   }

   private static int sizeToCapacityAgrona(int size) {
      return Math.max((int)((float)size / 0.55F) + 1, 16);
   }

   private static int sizeToCapacityJDK(int size) {
      return Math.max((int)((float)size / 0.75F) + 1, 16);
   }

   public static <T> Set<T> newSet() {
      return newSetOfCapacity(16);
   }

   public static <T> Set<T> setFromCollection(Collection<T> c) {
      Set<T> result = newSetForSize(c.size());
      result.addAll(c);
      return result;
   }

   public static <T> Set<T> setFromArray(T... c) {
      Set<T> result = newSetForSize(c.length);
      T[] var2 = c;
      int var3 = c.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         T e = var2[var4];
         result.add(e);
      }

      return result;
   }

   public static <T> Set<T> setFromKeys(Map<T, ?> map) {
      Set<T> result = newSetForSize(map.size());
      Iterator var2 = map.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<T, ?> e = (Entry)var2.next();
         result.add(e.getKey());
      }

      return result;
   }

   public static <T> Set<T> setFromValues(Map<?, T> map) {
      Set<T> result = newSetForSize(map.size());
      Iterator var2 = map.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<?, T> e = (Entry)var2.next();
         result.add(e.getValue());
      }

      return result;
   }

   public static <T> Set<T> setFromIterable(Iterable<T> iterable) {
      Set<T> result = newSet();
      Iterator var2 = iterable.iterator();

      while(var2.hasNext()) {
         T element =(T) var2.next();
         result.add(element);
      }

      return result;
   }

   private static class CorrectlySerializingSet<T> extends ObjectHashSet<T> {
      CorrectlySerializingSet(int capacity) {
         super(capacity, 0.55F, false);
      }

      Object writeReplace() {
         return new HashSet(this);
      }
   }
}
