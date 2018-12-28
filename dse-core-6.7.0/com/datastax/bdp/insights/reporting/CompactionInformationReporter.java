package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.events.CompactionEndedInformation;
import com.datastax.bdp.insights.events.CompactionStartedInformation;
import com.datastax.bdp.insights.events.SSTableCompactionInformation;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionEvent;
import org.apache.cassandra.db.compaction.CompactionEventListener;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionInformationReporter implements CompactionEventListener {
   private static final Logger logger = LoggerFactory.getLogger(GCInformationReporter.class);
   private final InsightsClient insightsClient;

   @Inject
   public CompactionInformationReporter(InsightsClient insightsClient) {
      this.insightsClient = insightsClient;
   }

   public void startReportingCompactionInformation() {
      CompactionManager.instance.addListener(this);
   }

   public void stopReportingCompactionInformation() {
      CompactionManager.instance.removeListener(this);
   }

   public void handleCompactionEvent(CompactionEvent event, CompactionIterator ci, Map<SSTableReader, AbstractCompactionStrategy> compactionStrategyMap, long sstableSizeBytes) {
      CompactionInfo info = ci.getCompactionInfo();
      List<SSTableCompactionInformation> sstables = new ArrayList(compactionStrategyMap.size());
      Iterator var8 = compactionStrategyMap.entrySet().iterator();

      while(var8.hasNext()) {
         Entry<SSTableReader, AbstractCompactionStrategy> entry = (Entry)var8.next();
         SSTableReader sstable = (SSTableReader)entry.getKey();
         AbstractCompactionStrategy strategy = (AbstractCompactionStrategy)entry.getValue();
         sstables.add(new SSTableCompactionInformation(sstable.getFilename(), sstable.getSSTableLevel(), sstable.getTotalRows(), sstable.descriptor.generation, sstable.descriptor.version.getVersion(), sstable.onDiskLength(), strategy.getName()));
      }

      try {
         switch(null.$SwitchMap$org$apache$cassandra$db$compaction$CompactionEvent[event.ordinal()]) {
         case 1:
            this.insightsClient.report(new CompactionStartedInformation(info.getTaskId(), (String)info.getKeyspace().orElse((Object)null), (String)info.getTable().orElse((Object)null), info.getTaskType(), info.getTotal(), ci.isStopRequested(), sstableSizeBytes, sstables));
            break;
         case 2:
            this.insightsClient.report(new CompactionEndedInformation(info.getTaskId(), (String)info.getKeyspace().orElse((Object)null), (String)info.getTable().orElse((Object)null), info.getTaskType(), info.getCompleted(), info.getTotal(), ci.isStopRequested(), ci.getTotalSourceCQLRows(), sstableSizeBytes, sstables));
         }
      } catch (Exception var12) {
         logger.warn("Error reporting compaction information", var12);
      }

   }
}
