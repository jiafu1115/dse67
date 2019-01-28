package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.utils.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeReducer extends Reducer<Unfiltered, Unfiltered> {
   private static final Logger logger = LoggerFactory.getLogger(MergeReducer.class);
   private final UnfilteredRowIterators.MergeListener listener;
   private Unfiltered.Kind nextKind;
   private final int size;
   private final int nowInSec;
   private Row.Merger rowMerger;
   private RangeTombstoneMarker.Merger markerMerger;
   private PartitionHeader header;

   public MergeReducer(int size, int nowInSec, PartitionHeader header, UnfilteredRowIterators.MergeListener listener) {
      this.size = size;
      this.markerMerger = null;
      this.listener = listener;
      this.nowInSec = nowInSec;

      assert header != null;

      Columns columns = header.columns.regulars;
      this.rowMerger = new Row.Merger(size, nowInSec, columns.size(), columns.hasComplex());
      this.header = header;
   }

   private void maybeInitMarkerMerger() {
      if(this.markerMerger == null) {
         this.markerMerger = new RangeTombstoneMarker.Merger(this.size, this.header.partitionLevelDeletion, this.header.isReverseOrder);
      }

   }

   public boolean trivialReduceIsTrivial() {
      return this.listener == null;
   }

   public void reduce(int idx, Unfiltered current) {
      this.nextKind = current.kind();
      switch (this.nextKind) {
         case ROW: {
            this.rowMerger.add(idx, (Row)current);
            break;
         }
         case RANGE_TOMBSTONE_MARKER: {
            this.maybeInitMarkerMerger();
            this.markerMerger.add(idx, (RangeTombstoneMarker)current);
         }
      }
   }

   @Override
   public Unfiltered getReduced() {
      switch (this.nextKind) {
         case ROW: {
            Row merged = this.rowMerger.merge(this.markerMerger == null ? this.header.partitionLevelDeletion : this.markerMerger.activeDeletion());
            if (merged == null) {
               return null;
            }
            if (this.listener != null) {
               this.listener.onMergedRows(merged, this.rowMerger.mergedRows());
            }
            return merged;
         }
         case RANGE_TOMBSTONE_MARKER: {
            this.maybeInitMarkerMerger();
            RangeTombstoneMarker merged = this.markerMerger.merge();
            if (this.listener != null) {
               this.listener.onMergedRangeTombstoneMarkers(merged, this.markerMerger.mergedMarkers());
            }
            return merged;
         }
      }
      throw new AssertionError();
   }

   @Override
   public void onKeyChange() {
      if (this.nextKind == null) {
         return;
      }
      switch (this.nextKind) {
         case ROW: {
            this.rowMerger.clear();
            break;
         }
         case RANGE_TOMBSTONE_MARKER: {
            this.markerMerger.clear();
         }
      }
   }

}
