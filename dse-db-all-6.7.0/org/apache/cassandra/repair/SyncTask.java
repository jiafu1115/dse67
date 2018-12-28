package org.apache.cassandra.repair;

import com.google.common.util.concurrent.AbstractFuture;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.RangeHash;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable {
   private static Logger logger = LoggerFactory.getLogger(SyncTask.class);
   protected final RepairJobDesc desc;
   private TreeResponse r1;
   private TreeResponse r2;
   protected final InetAddress endpoint1;
   protected final InetAddress endpoint2;
   protected final PreviewKind previewKind;
   private final Executor taskExecutor;
   private final SyncTask next;
   private final RepairSyncCache cache;
   protected volatile SyncStat stat;
   protected long startTime = -9223372036854775808L;

   public SyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2, Executor taskExecutor, SyncTask next, RepairSyncCache cache, PreviewKind previewKind) {
      this.desc = desc;
      this.r1 = r1;
      this.r2 = r2;
      this.endpoint1 = r1.endpoint;
      this.endpoint2 = r2.endpoint;
      this.previewKind = previewKind;
      this.taskExecutor = taskExecutor;
      this.next = next;
      this.cache = cache;
   }

   public void run() {
      this.startTime = ApolloTime.millisSinceStartup();

      try {
         List<MerkleTree.TreeDifference> diffs = MerkleTrees.diff(this.r1.trees, this.r2.trees);
         this.r1 = null;
         this.r2 = null;
         this.stat = new SyncStat(new NodePair(this.endpoint1, this.endpoint2), (long)diffs.size());
         String format = String.format("%s Endpoints %s and %s %%s for %s", new Object[]{this.previewKind.logPrefix(this.desc.sessionId), this.endpoint1, this.endpoint2, this.desc.columnFamily});
         if(diffs.isEmpty()) {
            logger.info(String.format(format, new Object[]{"are consistent"}));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}.", new Object[]{this.endpoint1, this.endpoint2, this.desc.columnFamily});
            this.set(this.stat);
            return;
         }

         List<Range<Token>> transferToLeft = new ArrayList(diffs.size());
         List<Range<Token>> transferToRight = new ArrayList(diffs.size());
         Iterator var5 = diffs.iterator();

         while(var5.hasNext()) {
            MerkleTree.TreeDifference treeDiff = (MerkleTree.TreeDifference)var5.next();
            RangeHash rightRangeHash = treeDiff.getRightRangeHash();
            RangeHash leftRangeHash = treeDiff.getLeftRangeHash();
            if(rightRangeHash.isNonEmpty() && this.cache.shouldSkip(this.endpoint2, this.endpoint1, rightRangeHash)) {
               logger.trace("Skipping transfer of already transferred range {} to {}.", treeDiff, this.endpoint1);
            } else {
               transferToLeft.add(treeDiff);
               this.cache.add(this.endpoint1, rightRangeHash);
            }

            if(leftRangeHash.isNonEmpty() && this.cache.shouldSkip(this.endpoint1, this.endpoint2, leftRangeHash)) {
               logger.trace("Skipping transfer of already transferred range {} to {}.", treeDiff, this.endpoint2);
            } else {
               transferToRight.add(treeDiff);
               this.cache.add(this.endpoint2, leftRangeHash);
            }
         }

         int skippedLeft = diffs.size() - transferToLeft.size();
         int skippedRight = diffs.size() - transferToRight.size();
         String skippedMsg = transferToLeft.size() == diffs.size() && transferToRight.size() == diffs.size()?"":String.format(" (%d and %d ranges skipped respectively).", new Object[]{Integer.valueOf(skippedLeft), Integer.valueOf(skippedRight)});
         logger.info(String.format(format, new Object[]{"have " + diffs.size() + " range(s) out of sync"}) + skippedMsg);
         Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}{}.", new Object[]{this.endpoint1, Integer.valueOf(diffs.size()), this.endpoint2, this.desc.columnFamily, skippedMsg});
         if(transferToLeft.isEmpty() && transferToRight.isEmpty()) {
            logger.info("[repair #{}] All differences between {} and {} already transferred for {}.", new Object[]{this.desc.sessionId, this.endpoint1, this.endpoint2, this.desc.columnFamily});
            this.set(this.stat);
            return;
         }

         this.startSync(transferToLeft, transferToRight);
      } catch (Throwable var12) {
         logger.info("[repair #{}] Error while calculating differences between {} and {}.", new Object[]{this.desc.sessionId, this.endpoint1, this.endpoint2, var12});
         this.setException(var12);
      } finally {
         if(this.next != null) {
            this.taskExecutor.execute(this.next);
         }

      }

   }

   public SyncStat getCurrentStat() {
      return this.stat;
   }

   protected abstract void startSync(List<Range<Token>> var1, List<Range<Token>> var2);

   protected void finished() {
      if(this.startTime != -9223372036854775808L) {
         Keyspace.open(this.desc.keyspace).getColumnFamilyStore(this.desc.columnFamily).metric.syncTime.update(ApolloTime.millisSinceStartupDelta(this.startTime), TimeUnit.MILLISECONDS);
      }

   }
}
