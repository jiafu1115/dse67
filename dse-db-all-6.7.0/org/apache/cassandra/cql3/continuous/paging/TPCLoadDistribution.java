package org.apache.cassandra.cql3.continuous.paging;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.utils.Pair;

public class TPCLoadDistribution {
   private final AtomicIntegerArray tasksPerCore = new AtomicIntegerArray(TPCUtils.getNumCores());

   public TPCLoadDistribution() {
   }

   public int incrementTasksForBestCore() {
      int currentCore = TPC.bestTPCCore();

      Pair preferred;
      do {
         preferred = this.getPreferredCore(currentCore);
      } while(!this.tasksPerCore.compareAndSet(((Integer)preferred.left).intValue(), ((Integer)preferred.right).intValue(), ((Integer)preferred.right).intValue() + 1));

      return ((Integer)preferred.left).intValue();
   }

   private Pair<Integer, Integer> getPreferredCore(int currentCore) {
      Pair<Integer, Integer> ret = Pair.create(Integer.valueOf(currentCore), Integer.valueOf(this.tasksPerCore.get(currentCore)));

      for(int i = 0; i < this.tasksPerCore.length() && ((Integer)ret.right).intValue() != 0; ++i) {
         int tasks = this.tasksPerCore.get(i);
         if(tasks < ((Integer)ret.right).intValue()) {
            ret = Pair.create(Integer.valueOf(i), Integer.valueOf(tasks));
         }
      }

      return ret;
   }

   public void decrementTasks(int coreId) {
      this.tasksPerCore.decrementAndGet(coreId);
   }

   @VisibleForTesting
   int getTasksForCore(int coreId) {
      return this.tasksPerCore.get(coreId);
   }

   public String toString() {
      return String.format("Load: %s", new Object[]{this.tasksPerCore});
   }
}
