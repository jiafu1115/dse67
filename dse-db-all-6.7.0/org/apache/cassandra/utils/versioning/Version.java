package org.apache.cassandra.utils.versioning;

public interface Version<V extends Enum<V>> {
   static <V extends Enum<V> & Version<V>> V last(Class<V> versionClass) {
      return (V)((Enum[])versionClass.getEnumConstants())[((Enum[])versionClass.getEnumConstants()).length - 1];
   }
}
