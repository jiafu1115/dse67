package org.apache.cassandra.utils.memory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.Timer;

public abstract class MemtablePool {
   private static final AtomicLongFieldUpdater<MemtablePool> NEXT_CLEAN_UPDATER = AtomicLongFieldUpdater.newUpdater(MemtablePool.class, "nextClean");
   private final AtomicReference<CompletableFuture<Void>> releaseFuture = new AtomicReference(new CompletableFuture());
   private final long cleanThresholdInBytes;
   private final long maxMemory;
   final MemtableCleanerThread<?> cleaner;
   final Timer blockedOnAllocating;
   public final MemtablePool.SubPool onHeap;
   public final MemtablePool.SubPool offHeap;
   private volatile long nextClean;

   MemtablePool(long maxMemory, double cleanThreshold, Runnable cleaner) {
      this.onHeap = new MemtablePool.SubPool(this, maxMemory);
      this.offHeap = new MemtablePool.SubPool(this, maxMemory);
      this.cleanThresholdInBytes = (long)((double)maxMemory * cleanThreshold);
      this.cleaner = cleaner == null?null:new MemtableCleanerThread(this, cleaner);
      this.blockedOnAllocating = CassandraMetricsRegistry.Metrics.timer((new DefaultNameFactory("MemtablePool")).createMetricName("BlockedOnAllocation"));
      if(this.cleaner != null) {
         this.cleaner.start();
      }

      this.maxMemory = maxMemory;
   }

   public abstract MemtableAllocator newAllocator(int var1);

   public CompletableFuture<Void> releaseFuture() {
      return (CompletableFuture)this.releaseFuture.get();
   }

   public Timer.Context blockedTimerContext() {
      return this.blockedOnAllocating.timer();
   }

   private void maybeClean() {
      if(this.needsCleaning() && this.cleaner != null) {
         this.cleaner.trigger();
      }

   }

   final boolean needsCleaning() {
      return this.used() > this.nextClean && this.updateNextClean();
   }

   public boolean belowLimit() {
      return this.used() <= this.maxMemory;
   }

   private boolean updateNextClean() {
      long current;
      long next;
      do {
         current = this.nextClean;
         long reclaiming = this.reclaiming();
         next = reclaiming + this.cleanThresholdInBytes;
      } while(current != next && !NEXT_CLEAN_UPDATER.compareAndSet(this, current, next));

      return this.used() > next;
   }

   private long reclaiming() {
      return this.onHeap.reclaiming + this.offHeap.reclaiming;
   }

   private long used() {
      return this.onHeap.used() + this.offHeap.used();
   }

   public static class SubPool {
      private static final AtomicLongFieldUpdater<MemtablePool.SubPool> RECLAIMING_UPDATER = AtomicLongFieldUpdater.newUpdater(MemtablePool.SubPool.class, "reclaiming");
      private static final AtomicLongFieldUpdater<MemtablePool.SubPool> ALLOCATED_UPDATER = AtomicLongFieldUpdater.newUpdater(MemtablePool.SubPool.class, "allocated");
      private final MemtablePool pool;
      final long limit;
      volatile long allocated;
      volatile long reclaiming;

      private SubPool(MemtablePool pool, long limit) {
         this.pool = pool;
         this.limit = limit;
      }

      private void adjustAllocated(long size) {
         ALLOCATED_UPDATER.addAndGet(this, size);
      }

      void allocated(long size) {
         assert size >= 0L;

         if(size != 0L) {
            this.adjustAllocated(size);
            this.pool.maybeClean();
         }
      }

      void released(long size) {
         assert size >= 0L;

         this.adjustAllocated(-size);
         CompletableFuture<Void> future = (CompletableFuture)this.pool.releaseFuture.getAndSet(new CompletableFuture());
         future.complete(null);
      }

      void reclaiming(long size) {
         if(size != 0L) {
            RECLAIMING_UPDATER.addAndGet(this, size);
         }
      }

      void reclaimed(long size) {
         if(size != 0L) {
            RECLAIMING_UPDATER.addAndGet(this, -size);
            if(this.pool.updateNextClean() && this.pool.cleaner != null) {
               this.pool.cleaner.trigger();
            }

         }
      }

      public long used() {
         return this.allocated;
      }

      public float reclaimingRatio() {
         float r = (float)this.reclaiming / (float)this.limit;
         return Float.isNaN(r)?0.0F:r;
      }

      public float usedRatio() {
         float r = (float)this.allocated / (float)this.limit;
         return Float.isNaN(r)?0.0F:r;
      }

      public MemtableAllocator.SubAllocator newAllocator() {
         return new MemtableAllocator.SubAllocator(this);
      }
   }
}
