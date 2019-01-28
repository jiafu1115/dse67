package org.apache.cassandra.dht;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairRangeStreamer extends RangeStreamer {
    private static final Logger logger = LoggerFactory.getLogger(RepairRangeStreamer.class);
    private static AtomicInteger cmd = new AtomicInteger(0);
    private static final int REPAIR_PARALLELISM = PropertyConfiguration.getInteger("dse.consistent_replace.parallelism", 2);
    private static final int REPAIR_RETRIES = PropertyConfiguration.getInteger("dse.consistent_replace.retries", 3);
    private static final int SPLITS_PER_RANGE = PropertyConfiguration.getInteger("dse.consistent_replace.splits_per_range", 0);
    static final long OPTIMAL_PARTITIONS_PER_SUBRANGE;
    private final TokenMetadata tokenMetadata;
    private final RangeStreamer.ISourceFilter filter;
    private final IEndpointSnitch snitch;
    private final BootStrapper.StreamConsistency consistency;
    private final Splitter splitter;
    private final SizeEstimates sizeEstimates;
    private final RepairRangeStreamer.Checkpoint checkpoint;
    private final RepairRangeStreamer.Throttle throttle;
    private final RepairRangeStreamer.Retry retry;
    private final Map<String, Set<String>> targetKsCfs;
    private Map<String, Map<InetAddress, Set<Range<Token>>>> toStream;

    RepairRangeStreamer(TokenMetadata metadata, Collection<Token> tokens, InetAddress address, StreamOperation streamOperation, boolean useStrictConsistency, IEndpointSnitch snitch, StreamStateStore stateStore, boolean connectSequentially, int connectionsPerHost, RangeStreamer.ISourceFilter sourceFilter) {
        super(metadata, tokens, address, streamOperation, useStrictConsistency, snitch, stateStore, connectSequentially, connectionsPerHost, sourceFilter);
        this.throttle = new RepairRangeStreamer.Throttle(REPAIR_PARALLELISM);
        this.retry = new RepairRangeStreamer.Retry(REPAIR_RETRIES);
        this.consistency = StorageService.getReplaceConsistency();
        this.tokenMetadata = metadata.cloneOnlyTokenMap();
        this.filter = sourceFilter;
        this.snitch = snitch;
        IPartitioner partitioner = this.tokenMetadata.partitioner;
        this.splitter = (Splitter) this.tokenMetadata.partitioner.splitter().orElse(null);
        this.sizeEstimates = new SizeEstimates(partitioner);
        this.checkpoint = new RepairRangeStreamer.Checkpoint(partitioner);
        this.targetKsCfs = getConsistentReplaceKeyspacesTables();
    }

    public ListenableFuture<StreamState> fetchAsync(StreamEventHandler handler) {
        ListenableFuture<StreamState> streamingFuture = super.fetchAsync(handler);
        ListenableFuture repairFuture = this.repairAsync();
        return Futures.transform(streamingFuture, (AsyncFunction) (streamState) -> {
            return Futures.transform(repairFuture, (AsyncFunction)(o) -> {
                return (ListenableFuture<StreamState>)streamState;
            });
        });
    }

    private ListenableFuture repairAsync() {
        if (!this.consistency.shouldRepair()) {
            logger.info("No consistent-replace repair task with stream consistency: {}", this.consistency);
            return Futures.immediateFuture(null);
        } else {
            logger.info("Starting consistent replace with consistency {}, {} and parallelism of {}.", new Object[]{this.consistency, SPLITS_PER_RANGE == 0 ? "split subranges by size estimates" : SPLITS_PER_RANGE + " splits per range", Integer.valueOf(REPAIR_PARALLELISM)});
            RepairRangeStreamer.ClusterRepairTask task = this.createRepairTask();
            return this.retry.runWithRetry(() -> {
                return task.repair();
            });
        }
    }

    @VisibleForTesting
    protected RepairRangeStreamer.ClusterRepairTask createRepairTask() {
        this.toStream = aggregate(this.toFetch());
        return new RepairRangeStreamer.ClusterRepairTask(this.toStream);
    }

    private static Map<String, Map<InetAddress, Set<Range<Token>>>> aggregate(Multimap<String, Entry<InetAddress, Collection<Range<Token>>>> toFetch) {
        Map<String, Map<InetAddress, Set<Range<Token>>>> res = new HashMap();
        Iterator var2 = toFetch.entries().iterator();

        while (var2.hasNext()) {
            Entry<String, Entry<InetAddress, Collection<Range<Token>>>> ksToFetch = (Entry) var2.next();
            String keyspace = (String) ksToFetch.getKey();
            InetAddress endpoint = (InetAddress) ((Entry) ksToFetch.getValue()).getKey();
            Collection<Range<Token>> ranges = (Collection) ((Entry) ksToFetch.getValue()).getValue();
            Map<InetAddress, Set<Range<Token>>> endpointRanges = (Map) res.computeIfAbsent(keyspace, (k) -> {
                return new HashMap();
            });
            Set<Range<Token>> allRanges = (Set) endpointRanges.computeIfAbsent(endpoint, (k) -> {
                return new HashSet();
            });
            allRanges.addAll(ranges);
        }

        return res;
    }

    public static Map<String, Set<String>> getConsistentReplaceKeyspacesTables() {
        String prop = System.getProperty("dse.consistent_replace.whitelist");
        if (Strings.isNullOrEmpty(prop)) {
            return Collections.emptyMap();
        } else {
            Map<String, Set<String>> keyspaceTables = new HashMap();
            String[] ksOrCfs = prop.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            String[] var3 = ksOrCfs;
            int var4 = ksOrCfs.length;

            for (int var5 = 0; var5 < var4; ++var5) {
                String ksOrCf = var3[var5];
                if (!Strings.isNullOrEmpty(ksOrCf)) {
                    String[] ksAndCf = ksOrCf.split("\\.(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 2);
                    Set<String> tables = (Set) keyspaceTables.computeIfAbsent(ksAndCf[0].trim(), (k) -> {
                        return new HashSet();
                    });
                    if (ksAndCf.length > 1 && !Strings.isNullOrEmpty(ksAndCf[1])) {
                        tables.add(ksAndCf[1].trim());
                    }
                }
            }

            return keyspaceTables;
        }
    }

    static {
        OPTIMAL_PARTITIONS_PER_SUBRANGE = (long) Math.pow(2.0D, (double) CompactionManager.MAX_MERKLE_TREE_DEPTH);
    }

    static class Checkpoint {
        private volatile Map<String, Collection<Range<Token>>> failedRepair = new ConcurrentHashMap();
        private final IPartitioner partitioner;

        Checkpoint(IPartitioner partitioner) {
            this.partitioner = partitioner;
        }

        public boolean succeed() {
            return this.failedRepair.isEmpty();
        }

        public ListenableFuture runWithCheckpoint(final String keyspace, final Collection<Range<Token>> ranges, Supplier<ListenableFuture> taskSupplier) {
            final SettableFuture ret = SettableFuture.create();
            Futures.addCallback((ListenableFuture) taskSupplier.get(), new FutureCallback<Object>() {
                public void onSuccess(@Nullable Object o) {
                    try {
                        RepairRangeStreamer.Checkpoint.saveRepairedRanges(keyspace, ranges);
                    } finally {
                        ret.set(null);
                    }

                }

                public void onFailure(Throwable throwable) {
                    try {
                        RepairRangeStreamer.logger.error("Failed to repair keyspace {} with range {} due to {}", new Object[]{keyspace, ranges, throwable.getMessage(), throwable});
                        ((Collection) Checkpoint.this.failedRepair.computeIfAbsent(keyspace, (k) -> {
                            return Collections.synchronizedList(new ArrayList());
                        })).addAll(ranges);
                    } finally {
                        ret.set(null);
                    }

                }
            });
            return ret;
        }

        public Collection<Range<Token>> filterRepairedRanges(String keyspace, Collection<Range<Token>> ranges) {
            Collection<Range<Token>> filtered = new ArrayList(ranges);
            Set repairedRanges = Collections.emptySet();

            try {
                repairedRanges = this.getRepairedRanges(keyspace);
            } catch (InterruptedException | ExecutionException var6) {
                RepairRangeStreamer.logger.error("Unable to fetch repaired ranges: {}", var6.getMessage(), var6);
            }

            if (filtered.removeAll(repairedRanges)) {
                RepairRangeStreamer.logger.info("Some ranges of {} are already repaired for {}. Skipping repairing those ranges.", repairedRanges, keyspace);
            }

            return filtered;
        }

        Set<Range<Token>> getRepairedRanges(String keyspace) throws ExecutionException, InterruptedException {
            return (Set) ((Map) SystemKeyspace.getTransferredRanges(StreamOperation.REPAIR.getDescription(), keyspace, this.partitioner).get()).getOrDefault(FBUtilities.getBroadcastAddress(), Collections.emptySet());
        }

        private static void saveRepairedRanges(String keyspace, Collection<Range<Token>> ranges) {
            SystemKeyspace.updateTransferredRanges(StreamOperation.REPAIR, FBUtilities.getBroadcastAddress(), keyspace, ranges);
        }

        public String toString() {
            return this.failedRepair.toString();
        }

        public void clearFailed() {
            this.failedRepair.clear();
        }
    }

    static class Throttle {
        private final Semaphore semaphore;

        public Throttle(int parallelism) {
            this.semaphore = new Semaphore(parallelism);
        }

        public ListenableFuture runWithThrottle(Supplier<ListenableFuture> taskSupplier) {
            this.acquire();
            ListenableFuture ret = (ListenableFuture) taskSupplier.get();
            ret.addListener(() -> {
                this.release();
            }, MoreExecutors.directExecutor());
            return ret;
        }

        private void acquire() {
            try {
                this.semaphore.acquire();
            } catch (InterruptedException var2) {
                throw new RuntimeException(var2);
            }
        }

        private void release() {
            this.semaphore.release();
        }
    }

    static class Retry {
        private final int maxRetries;

        public Retry(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public ListenableFuture runWithRetry(Supplier<ListenableFuture> taskSupplier) {
            return this.maxRetries == 0 ? (ListenableFuture) taskSupplier.get() : this.retry(SettableFuture.create(), taskSupplier, this.maxRetries);
        }

        private ListenableFuture retry(final SettableFuture ret, final Supplier<ListenableFuture> taskSupplier, final int remain) {
            ListenableFuture task = (ListenableFuture) taskSupplier.get();
            Futures.addCallback(task, new FutureCallback<Object>() {
                public void onSuccess(Object result) {
                    ret.set(null);
                }

                public void onFailure(Throwable t) {
                    RepairRangeStreamer.logger.info("Will retry failed repair({}), remaining attempts: {}", t.getMessage(), Integer.valueOf(remain));
                    if (remain > 0) {
                        Retry.this.retry(ret, taskSupplier, remain - 1);
                    } else {
                        ret.setException(t);
                    }

                }
            });
            return ret;
        }
    }

    private class TableSubrangeRepairTask {
        private final String keyspace;
        private final String table;
        private final Set<InetAddress> neighbors;
        private final Collection<Range<Token>> subranges;

        TableSubrangeRepairTask(String keyspace, String table, Set<InetAddress> neighbors, Collection<Range<Token>> subranges) {
            this.keyspace = keyspace;
            this.table = table;
            this.neighbors = neighbors;
            this.subranges = subranges;
        }

        private ListenableFuture repair() {
            return RepairRangeStreamer.this.throttle.runWithThrottle(() -> {
                return this.repairAsync();
            });
        }

        private ListenableFuture repairAsync() {
            RepairOption options = new RepairOption(RepairParallelism.PARALLEL, false, false, false, 1, this.subranges, false, false, PreviewKind.NONE);
            this.neighbors.forEach((h) -> {
                options.getHosts().add(h.getHostAddress());
            });
            options.getColumnFamilies().add(this.table);
            RepairRangeStreamer.logger.info("Repairing {}.{} for range {} on peers {}", new Object[]{this.keyspace, this.table, options.getRanges(), options.getHosts()});
            RepairRunnable repairRunnable = new RepairRunnable(StorageService.instance, RepairRangeStreamer.cmd.incrementAndGet(), options, this.keyspace, true, true, true, true, (Map) RepairRangeStreamer.this.toStream.getOrDefault(this.keyspace, Collections.emptyMap()));
            repairRunnable.run();
            return repairRunnable.getResult();
        }
    }

    public class TableRepairTask {
        final String keyspace;
        final String table;
        final Set<InetAddress> neighbors;
        final Collection<Range<Token>> ranges;

        TableRepairTask(String keyspace, String table, Set<InetAddress> neighbors, Collection<Range<Token>> commonRanges) {
            this.keyspace = keyspace;
            this.table = table;
            this.neighbors = neighbors;
            this.ranges = commonRanges;
        }

        @VisibleForTesting
        public boolean shouldRepair() {
            return RepairRangeStreamer.this.targetKsCfs.isEmpty() || RepairRangeStreamer.this.targetKsCfs.containsKey(this.keyspace) && (((Set) RepairRangeStreamer.this.targetKsCfs.get(this.keyspace)).isEmpty() || ((Set) RepairRangeStreamer.this.targetKsCfs.get(this.keyspace)).contains(this.table));
        }

        public ListenableFuture repair() {
            ListenableFuture future = Futures.immediateFuture(null);
            if (!this.shouldRepair()) {
                logger.debug("{}.{} is skipped for consistent replace.", (Object)this.keyspace, (Object)this.table);
                return future;
            }
            for (TableSubrangeRepairTask task : this.createSubrangeTasks()) {
                future = Futures.transform((ListenableFuture)future, (AsyncFunction) o -> task.repair());
            }
            return future;
        }

        public List<RepairRangeStreamer.TableSubrangeRepairTask> createSubrangeTasks() {
            List<RepairRangeStreamer.TableSubrangeRepairTask> tasks = new ArrayList();
            Collection<List<Range<Token>>> groups = this.groupByOptimalMerkleSize(RepairRangeStreamer.this.sizeEstimates);
            Iterator var3 = groups.iterator();

            while (var3.hasNext()) {
                List<Range<Token>> subranges = (List) var3.next();
                tasks.add(RepairRangeStreamer.this.new TableSubrangeRepairTask(this.keyspace, this.table, this.neighbors, subranges));
            }

            return tasks;
        }

        Collection<List<Range<Token>>> groupByOptimalMerkleSize(final SizeEstimates estimates) {
            if (RepairRangeStreamer.this.splitter == null) {
                return this.ranges.stream().map(Collections::singletonList).collect(Collectors.toList());
            }
            final Collection<Pair<Range<Token>, Long>> rangePartitions = this.ranges.stream().map(r -> Pair.create(r, estimates.getEstimatedPartitions(this.keyspace, this.table, this.neighbors, r))).sorted(Comparator.comparingLong(p -> (long)p.right)).collect(Collectors.toList());
            final List<List<Range<Token>>> groups = new ArrayList();
            int index = 0;
            long count = 0L;
            for (final Pair<Range<Token>, Long> pair : rangePartitions) {
                if (pair.right > RepairRangeStreamer.OPTIMAL_PARTITIONS_PER_SUBRANGE) {
                    final List<Range<Token>> splitRanges = this.splitSubranges(pair.left, pair.right, this.keyspace, this.table);
                    splitRanges.forEach(r -> groups.add(Collections.singletonList(r)));
                    count = 0L;
                    index = groups.size();
                }
                else {
                    count += pair.right;
                    if (count < 0L || count > RepairRangeStreamer.OPTIMAL_PARTITIONS_PER_SUBRANGE) {
                        ++index;
                        count = pair.right;
                    }
                    if (index >= groups.size()) {
                        groups.add(new ArrayList<Range<Token>>());
                    }
                    groups.get(index).add(pair.left);
                }
            }
            return groups;
        }

        private int getOptimalSplits(long estimatedPartitions) {
            assert RepairRangeStreamer.this.splitter != null;

            int splits;
            if (RepairRangeStreamer.SPLITS_PER_RANGE != 0) {
                splits = RepairRangeStreamer.SPLITS_PER_RANGE;
            } else {
                splits = Math.max(1, (int) ((estimatedPartitions + RepairRangeStreamer.OPTIMAL_PARTITIONS_PER_SUBRANGE - 1L) / RepairRangeStreamer.OPTIMAL_PARTITIONS_PER_SUBRANGE));
            }

            return splits;
        }

        private List<Range<Token>> splitSubranges(Range<Token> range, long estimatedPartitions, String keyspace, String table) {
            assert RepairRangeStreamer.this.splitter != null;

            int splits = this.getOptimalSplits(estimatedPartitions);
            if (splits > 1) {
                RepairRangeStreamer.logger.debug("Splitting range {} with estimated partitions: {} into {} subranges for {}.{}", new Object[]{range, Long.valueOf(estimatedPartitions), Integer.valueOf(splits), keyspace, table});
            }

            return RepairRangeStreamer.this.splitter.splitEvenly(range, splits);
        }
    }

    public class KeyspaceCommonRangeRepairTask {
        final String keyspace;
        final Collection<Range<Token>> commonRanges;
        final Set<InetAddress> neighbors;

        public KeyspaceCommonRangeRepairTask(String keyspace, Collection<Range<Token>> commonRanges, Set<InetAddress> neighbors) {
            this.keyspace = keyspace;
            this.commonRanges = commonRanges;
            this.neighbors = neighbors;
        }

        public ListenableFuture repair() {
            return RepairRangeStreamer.this.checkpoint.runWithCheckpoint(this.keyspace, this.commonRanges, () -> {
                return this.repairTables();
            });
        }


        private ListenableFuture repairTables() {
            ListenableFuture future = Futures.immediateFuture((Object) null);
            for (final TableRepairTask task : this.createTableRepairs()) {
                future = Futures.transform(future, (AsyncFunction) o -> task.repair());
            }
            return future;
        }

        List<RepairRangeStreamer.TableRepairTask> createTableRepairs() {
            List<RepairRangeStreamer.TableRepairTask> tasks = new ArrayList();
            Keyspace ks = Keyspace.open(this.keyspace);

            for(ColumnFamilyStore cfs:ks.getColumnFamilyStores()){
                tasks.add(RepairRangeStreamer.this.new TableRepairTask(this.keyspace, cfs.name, this.neighbors, this.commonRanges));
            }

            return tasks;
        }
    }

    public class KeyspaceRepairTask {
        final Keyspace ks;
        final String keyspace;
        final Multimap<Range<Token>, InetAddress> rangeAddresses;
        final Collection<Range<Token>> localRanges;

        public KeyspaceRepairTask(String keyspace, Collection<Range<Token>> localRanges) {
            this.ks = Keyspace.open(keyspace);
            this.keyspace = keyspace;
            this.rangeAddresses = this.ks.getReplicationStrategy().getRangeAddresses(RepairRangeStreamer.this.tokenMetadata);
            this.localRanges = localRanges;
        }

        @VisibleForTesting
        public boolean shouldRepair() {
            return RepairRangeStreamer.this.targetKsCfs.isEmpty() || RepairRangeStreamer.this.targetKsCfs.containsKey(this.keyspace);
        }

        public ListenableFuture repair() {
            if (!this.shouldRepair()) {
                RepairRangeStreamer.logger.debug("{} is skipped for consistent replace.", this.keyspace);
                return Futures.immediateFuture(null);
            } else {
                RepairRangeStreamer.logger.info("Consistent-replace repair on keyspace: {}", this.keyspace);
                List<ListenableFuture<Object>> futures = new ArrayList();

                for(RepairRangeStreamer.KeyspaceCommonRangeRepairTask task:this.createCommonRangeTasks()){
                    futures.add(task.repair());
                }

                return Futures.allAsList(futures);
            }
        }

        List<RepairRangeStreamer.KeyspaceCommonRangeRepairTask> createCommonRangeTasks() {
            List<Pair<Set<InetAddress>, Set<Range<Token>>>> commonRanges = this.getCommonRanges();
            Collections.shuffle(commonRanges);
            List<RepairRangeStreamer.KeyspaceCommonRangeRepairTask> tasks = new ArrayList();

            for (Pair<Set<InetAddress>, Set<Range<Token>>> p : commonRanges) {
                Set<InetAddress> neighbors = (Set) p.left;
                Set<Range<Token>> ranges = (Set) p.right;
                tasks.add(RepairRangeStreamer.this.new KeyspaceCommonRangeRepairTask(this.keyspace, ranges, neighbors));
            }

            return tasks;
        }

        List<Pair<Set<InetAddress>, Set<Range<Token>>>> getCommonRanges() {
            Collection<Range<Token>> ranges = RepairRangeStreamer.this.checkpoint.filterRepairedRanges(this.keyspace, this.localRanges);
            Map<Collection<InetAddress>, List<Range<Token>>> rangesByHosts = this.groupByCommonReplicas(ranges);
            Map<Set<InetAddress>, Set<Range<Token>>> peerCommonRanges = new HashMap();

            for (Entry<Collection<InetAddress>, List<Range<Token>>> entry : rangesByHosts.entrySet()) {
                Collection<InetAddress> peers = (Collection) entry.getKey();
                List<Range<Token>> commonRanges = (List) entry.getValue();

                assert peers != null && !peers.isEmpty() : String.format("Expect at least one peer for keyspace %s ranges %s", new Object[]{this.ks.getName(), commonRanges});

                Set<InetAddress> neighbors = this.getRepairCandidates(this.ks, peers, commonRanges);
                if (!neighbors.isEmpty()) {
                    ((Set) peerCommonRanges.computeIfAbsent(neighbors, (k) -> {
                        return new HashSet();
                    })).addAll(commonRanges);
                }
            }

            return (List) peerCommonRanges.entrySet().stream().map((e) -> {
                return Pair.create(e.getKey(), e.getValue());
            }).collect(Collectors.toList());
        }

        private Map<Collection<InetAddress>, List<Range<Token>>> groupByCommonReplicas(Collection<Range<Token>> localRanges) {
            return (Map) (localRanges.stream()).collect(Collectors.groupingBy(this.rangeAddresses::get));
        }

        private Set<InetAddress> getRepairCandidates(final Keyspace ks, final Collection<InetAddress> peers, final Collection<Range<Token>> ranges) {
            final List<InetAddress> sorted = RepairRangeStreamer.this.snitch.getSortedListByProximity(FBUtilities.getBroadcastAddress(), peers);
            final String keyspace = ks.getName();
            final AbstractReplicationStrategy strat = ks.getReplicationStrategy();
            final int required = RepairRangeStreamer.this.consistency.requiredSources(ks);
            final Set<InetAddress> neighbors = sorted.stream().filter(p -> !RepairRangeStreamer.this.consistency.shouldSkipSource(strat, p)).filter(RepairRangeStreamer.this.filter::shouldInclude).limit(required).collect(Collectors.toSet());
            if (strat.getReplicationFactor() == 1) {
                RepairRangeStreamer.logger.warn("Unable to find sufficient sources to repair range {} in keyspace {} with RF=1. Keyspace might be missing data.", (Object) ranges, (Object) keyspace);
                return Collections.emptySet();
            }
            if (strat.getReplicationFactor() == 2) {
                RepairRangeStreamer.logger.warn("Cannot ensure replace consistency {} for range {} in keyspace {} (RF={}). Required sources: {}, found sources {}.", new Object[]{RepairRangeStreamer.this.consistency, ranges, keyspace, strat.getReplicationFactor(), required, neighbors.size()});
                return Collections.emptySet();
            }
            if (neighbors.size() < required) {
                throw new RuntimeException(String.format("Required %d sources but got %d for keyspace %s(rf=%d) of range %s", required, neighbors.size(), keyspace, strat.getReplicationFactor(), ranges));
            }
            return neighbors;
        }
    }

    public class ClusterRepairTask {
        final Map<String, Map<InetAddress, Set<Range<Token>>>> toStream;

        public ClusterRepairTask(Map<String, Map<InetAddress, Set<Range<Token>>>> toFetch) {
            this.toStream = toFetch;
        }

        public ListenableFuture repair() {
            return Futures.transform(this.repairKeyspaces(), (AsyncFunction) (o) -> {
                RepairRangeStreamer.logger.info("RepairRangeStreamer finished {}", RepairRangeStreamer.this.checkpoint.succeed() ? "successfully" : "with error.");
                if (RepairRangeStreamer.this.checkpoint.succeed()) {
                    return Futures.immediateFuture(null);
                } else {
                    String msg = RepairRangeStreamer.this.checkpoint.toString();
                    RepairRangeStreamer.this.checkpoint.clearFailed();
                    return Futures.immediateFailedFuture(new RuntimeException(String.format("Failed to repair %s", new Object[]{msg})));
                }
            });
        }

        private ListenableFuture repairKeyspaces() {
            List<ListenableFuture<Object>> futures = new ArrayList();
            Iterator var2 = this.createKeyspaceRepairs().iterator();

            while (var2.hasNext()) {
                RepairRangeStreamer.KeyspaceRepairTask task = (RepairRangeStreamer.KeyspaceRepairTask) var2.next();
                futures.add(task.repair());
            }

            return Futures.allAsList(futures);
        }

        @VisibleForTesting
        protected List<RepairRangeStreamer.KeyspaceRepairTask> createKeyspaceRepairs() {
            return (List) this.toStream.keySet().stream().map((keyspace) -> {
                Set<Range<Token>> ranges = new HashSet();
                Iterator var3 = ((Map) this.toStream.get(keyspace)).entrySet().iterator();

                while (var3.hasNext()) {
                    Entry<InetAddress, Set<Range<Token>>> e = (Entry) var3.next();
                    ranges.addAll((Collection) e.getValue());
                }

                return RepairRangeStreamer.this.new KeyspaceRepairTask(keyspace, ranges);
            }).collect(Collectors.toList());
        }
    }
}
