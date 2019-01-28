package org.apache.cassandra.tools;

import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;

public class SSTableOfflineRelevel {
   public SSTableOfflineRelevel() {
   }

   public static void main(String[] args) throws IOException {
      PrintStream out = System.out;
      if (args.length < 2) {
         out.println("This command should be run with Cassandra stopped!");
         out.println("Usage: sstableofflinerelevel [--dry-run] <keyspace> <columnfamily>");
         System.exit(1);
      }
      Util.initDatabaseDescriptor();
      boolean dryRun = args[0].equals("--dry-run");
      String keyspace = args[args.length - 2];
      String columnfamily = args[args.length - 1];
      Schema.instance.loadFromDisk(false);
      if (Schema.instance.getTableMetadataRef(keyspace, columnfamily) == null) {
         throw new IllegalArgumentException(String.format("Unknown keyspace/columnFamily %s.%s", keyspace, columnfamily));
      }
      Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
      ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnfamily);
      Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
      HashMultimap<File,SSTableReader> sstableMultimap = HashMultimap.create();
      for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet()) {
         if (sstable.getKey() == null) continue;
         try {
            SSTableReader reader = SSTableReader.open(sstable.getKey());
            sstableMultimap.put(reader.descriptor.directory, reader);
         }
         catch (Throwable t) {
            out.println("Couldn't open sstable: " + sstable.getKey().filenameFor(Component.DATA));
            throw Throwables.propagate((Throwable)t);
         }
      }
      if (sstableMultimap.isEmpty()) {
         out.println("No sstables to relevel for " + keyspace + "." + columnfamily);
         System.exit(1);
      }
      for (File directory : sstableMultimap.keySet()) {
         if (sstableMultimap.get(directory).isEmpty()) continue;
         Relevel rl = new Relevel(sstableMultimap.get(directory));
         out.println("For sstables in " + directory + ":");
         rl.relevel(dryRun);
      }
      System.exit(0);
   }

   private static class Relevel {
      private final Set<SSTableReader> sstables;
      private final int approxExpectedLevels;

      public Relevel(Set<SSTableReader> sstables) {
         this.sstables = sstables;
         this.approxExpectedLevels = (int)Math.ceil(Math.log10(sstables.size()));
      }

      private void printLeveling(Iterable<SSTableReader> sstables) {
         ArrayListMultimap leveling = ArrayListMultimap.create();
         int maxLevel = 0;
         for (SSTableReader sstable : sstables) {
            leveling.put((Object)sstable.getSSTableLevel(), (Object)sstable);
            maxLevel = Math.max(sstable.getSSTableLevel(), maxLevel);
         }
         System.out.println("Current leveling:");
         for (int i = 0; i <= maxLevel; ++i) {
            System.out.println(String.format("L%d=%d", i, leveling.get((Object)i).size()));
         }
      }

      public void relevel(boolean dryRun) throws IOException {
         this.printLeveling(this.sstables);
         ArrayList<SSTableReader> sortedSSTables = new ArrayList<SSTableReader>(this.sstables);
         Collections.sort(sortedSSTables, new Comparator<SSTableReader>(){

            @Override
            public int compare(SSTableReader o1, SSTableReader o2) {
               return o1.last.compareTo(o2.last);
            }
         });
         List<List<SSTableReader>> levels = new ArrayList();
         while (!sortedSSTables.isEmpty()) {
            Iterator<SSTableReader> it = sortedSSTables.iterator();
            ArrayList<SSTableReader> level = new ArrayList<SSTableReader>();
            DecoratedKey lastLast = null;
            while (it.hasNext()) {
               SSTableReader sstable = it.next();
               if (lastLast != null && lastLast.compareTo(sstable.first) >= 0) continue;
               level.add(sstable);
               lastLast = sstable.last;
               it.remove();
            }
            levels.add(level);
         }
         ArrayList<SSTableReader> l0 = new ArrayList();
         if (this.approxExpectedLevels < levels.size()) {
            for (int i = this.approxExpectedLevels; i < levels.size(); ++i) {
               l0.addAll(levels.get(i));
            }
            levels = levels.subList(0, this.approxExpectedLevels);
         }
         if (dryRun) {
            System.out.println("Potential leveling: ");
         } else {
            System.out.println("New leveling: ");
         }
         System.out.println("L0=" + l0.size());
         for (int i = levels.size() - 1; i >= 0; --i) {
            System.out.println(String.format("L%d=%d", levels.size() - i, ((List)levels.get(i)).size()));
         }
         if (!dryRun) {
            for (SSTableReader sstable : l0) {
               if (sstable.getSSTableLevel() == 0) continue;
               sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
            }
            for (int i = levels.size() - 1; i >= 0; --i) {
               for (SSTableReader sstable : levels.get(i)) {
                  int newLevel = levels.size() - i;
                  if (newLevel == sstable.getSSTableLevel()) continue;
                  sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, newLevel);
               }
            }
         }
      }

   }
}
