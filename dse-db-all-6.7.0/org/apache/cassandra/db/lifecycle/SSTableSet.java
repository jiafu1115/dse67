package org.apache.cassandra.db.lifecycle;

public enum SSTableSet {
   CANONICAL,
   LIVE,
   NONCOMPACTING;

   private SSTableSet() {
   }
}
