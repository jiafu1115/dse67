package org.apache.cassandra.repair;

import java.util.List;

public class RepairResult {
   public final RepairJobDesc desc;
   public final List<SyncStat> stats;

   public RepairResult(RepairJobDesc desc, List<SyncStat> stats) {
      this.desc = desc;
      this.stats = stats;
   }
}
