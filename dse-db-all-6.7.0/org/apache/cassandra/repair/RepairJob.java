package org.apache.cassandra.repair;

import com.google.common.collect.Iterables;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairJob extends AbstractFuture<RepairResult> implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(RepairJob.class);
    private final RepairSession session;
    private final RepairJobDesc desc;
    private final RepairParallelism parallelismDegree;
    private final ListeningExecutorService taskExecutor;
    private final boolean isIncremental;
    private final PreviewKind previewKind;
    private int MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS = 3;
    private final InetAddress pivot;

    public RepairJob(RepairSession session, String columnFamily, boolean isIncremental, PreviewKind previewKind) {
        this.session = session;
        this.desc = new RepairJobDesc(session.parentRepairSession, session.getId(), session.keyspace, columnFamily, session.getRanges());
        this.taskExecutor = session.taskExecutor;
        this.parallelismDegree = session.parallelismDegree;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
        this.pivot = session.pullRemoteDiff ? (InetAddress) Iterables.get(session.endpoints, 0) : null;
    }

    public void run() {
        List<ListenableFuture<TreeResponse>> validations;
        Keyspace ks = Keyspace.open(this.desc.keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(this.desc.columnFamily);
        cfs.metric.repairsStarted.inc();
        ArrayList<InetAddress> allEndpoints = new ArrayList<InetAddress>(this.session.endpoints);
        if (!this.session.pullRemoteDiff) {
            allEndpoints.add(FBUtilities.getBroadcastAddress());
        }
        if (this.parallelismDegree != RepairParallelism.PARALLEL) {
            ListenableFuture snapshotResult;
            ArrayList<SnapshotTask> snapshotTasks = new ArrayList<SnapshotTask>(allEndpoints.size());
            if (this.isIncremental) {
                snapshotResult = Futures.immediateFuture(allEndpoints);
            } else {
                for (InetAddress endpoint : allEndpoints) {
                    SnapshotTask snapshotTask = new SnapshotTask(this.desc, endpoint);
                    snapshotTasks.add(snapshotTask);
                    this.taskExecutor.execute((Runnable) snapshotTask);
                }
                snapshotResult = Futures.allAsList(snapshotTasks);
            }
            try {
                List endpoints = (List) snapshotResult.get();
                if (this.parallelismDegree == RepairParallelism.SEQUENTIAL) {
                    validations = this.sendSequentialValidationRequest(endpoints);
                }
                validations = this.sendDCAwareValidationRequest(endpoints);
            } catch (Throwable t2) {
                JVMStabilityInspector.inspectThrowable(t2);
                this.waitForRemainingTasksAndFail("snapshot", snapshotTasks, t2);
                return;
            }
        } else {
            validations = this.sendValidationRequest(allEndpoints);
        }
        RepairSyncCache syncCache = new RepairSyncCache(this.session.skipFetching);
        ArrayList<SyncTask> syncTasks = new ArrayList<SyncTask>();
        try {
            List<TreeResponse> trees = Futures.allAsList(validations).get();
            ArrayList<List<TreeResponse>> treesByDc = new ArrayList<List<TreeResponse>>(
                    trees.stream().collect(Collectors.groupingBy(t -> DatabaseDescriptor.getEndpointSnitch().getDatacenter(t.endpoint))).values());
            SyncTask previous = null;
            for (int i = 0; i < treesByDc.size(); ++i) {
                for (int j = i + 1; j < treesByDc.size(); ++j) {
                    for (TreeResponse r1 : treesByDc.get(i)) {
                        for (TreeResponse r2 : treesByDc.get(j)) {
                            previous = this.createSyncTask(syncCache, syncTasks, previous, r1, r2);
                        }
                    }
                }
            }
            for (List<TreeResponse> localDc : treesByDc) {
                for (int i = 0; i < localDc.size(); ++i) {
                    TreeResponse r1;
                    r1 = localDc.get(i);
                    for (int j = i + 1; j < localDc.size(); ++j) {
                        TreeResponse r2;
                        r2 = localDc.get(j);
                        previous = this.createSyncTask(syncCache, syncTasks, previous, r1, r2);
                    }
                }
            }
            if (this.session.pullRemoteDiff) {
                logger.info("[repair #{}] created {} local sync tasks with {} endpoints for pulling remote diff", new Object[]{this.session.getId(), syncTasks.size(), allEndpoints.size()});
                assert (syncTasks.size() == (trees.size() - 1) * 2);
            } else assert (syncTasks.size() == IntMath.binomial((int) trees.size(), (int) 2));
            if (previous != null) {
                this.taskExecutor.submit(previous);
            }
        } catch (Throwable t3) {
            JVMStabilityInspector.inspectThrowable(t3);
            if (this.parallelismDegree == RepairParallelism.SEQUENTIAL) {
                this.failJob(t3);
            } else {
                this.waitForRemainingTasksAndFail("validation", validations, t3);
            }
            return;
        }
        try {
            List stats = (List) Futures.allAsList(syncTasks).get();
            if (!this.previewKind.isPreview()) {
                logger.info("[repair #{}] {} is fully synced", (Object) this.session.getId(), (Object) this.desc.columnFamily);
                SystemDistributedKeyspace.successfulRepairJob(this.session.getId(), this.desc.keyspace, this.desc.columnFamily);
            }
            cfs.metric.repairsCompleted.inc();
            this.set(new RepairResult(this.desc, stats));
        } catch (Throwable t4) {
            cfs.metric.repairsCompleted.inc();
            JVMStabilityInspector.inspectThrowable(t4);
            this.waitForRemainingTasksAndFail("sync", syncTasks, t4);
            return;
        }
    }

    private SyncTask createSyncTask(RepairSyncCache syncCache, List<SyncTask> syncTasks, SyncTask previous, TreeResponse r1, TreeResponse r2) {
        InetAddress local = FBUtilities.getLocalAddress();
        if (this.session.pullRemoteDiff) {
            assert !r1.endpoint.equals(local) && !r2.endpoint.equals(local);

            if (!r1.endpoint.equals(this.pivot) && !r2.endpoint.equals(this.pivot)) {
                return previous;
            } else {
                UUID sessionId = this.isIncremental ? this.desc.parentSessionId : null;
                SyncTask task = new LocalSyncTask(this.desc, r1.withEndpoint(local), r2, sessionId, true, this.session.keepLevel, this.taskExecutor, previous, syncCache, this.previewKind);
                syncTasks.add(task);
                task = new LocalSyncTask(this.desc, r1, r2.withEndpoint(local), sessionId, true, this.session.keepLevel, this.taskExecutor, task, syncCache, this.previewKind);
                syncTasks.add(task);
                return task;
            }
        } else {
            SyncTask task;
            if (!r1.endpoint.equals(local) && !r2.endpoint.equals(local)) {
                task = new RemoteSyncTask(this.desc, r1, r2, this.session, this.taskExecutor, previous, syncCache, this.previewKind);
            } else {
                task = new LocalSyncTask(this.desc, r1, r2, this.isIncremental ? this.desc.parentSessionId : null, this.session.pullRepair, this.session.keepLevel, this.taskExecutor, previous, syncCache, this.previewKind);
            }

            syncTasks.add(task);
            return (SyncTask) task;
        }
    }

    private <T> void waitForRemainingTasksAndFail(String phase, Iterable<? extends ListenableFuture<? extends T>> tasks, Throwable t) {
        if (!this.previewKind.isPreview()) {
            logger.warn("[{}] [repair #{}] {} {} failed. Will wait a maximum of {} hours for remaining tasks to finish.", new Object[]{this.session.parentRepairSession, this.session.getId(), this.desc.columnFamily, phase, Integer.valueOf(this.MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS), t});
        }

        try {
            Futures.successfulAsList(tasks).get((long) this.MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS, TimeUnit.HOURS);
            if (!this.previewKind.isPreview()) {
                logger.debug("[{}][{}] All remaining repair tasks finished.", this.session.parentRepairSession, this.session.getId());
            }
        } catch (Throwable var5) {
            JVMStabilityInspector.inspectThrowable(var5);
            if (!this.previewKind.isPreview()) {
                logger.warn("[{}] Exception while waiting for remaining repair tasks to complete.", this.session.parentRepairSession, var5);
            }
        }

        this.failJob(t);
    }

    private void failJob(Throwable t) {
        if (!this.previewKind.isPreview()) {
            SystemDistributedKeyspace.failedRepairJob(this.session.getId(), this.desc.keyspace, this.desc.columnFamily, t);
        }

        this.setException(t);
    }

    private List<ListenableFuture<TreeResponse>> sendValidationRequest(Collection<InetAddress> endpoints) {
        String message = String.format("Requesting merkle trees for %s (to %s)", new Object[]{this.desc.columnFamily, endpoints});
        logger.info("[repair #{}] {}", this.desc.sessionId, message);
        Tracing.traceRepair(message, new Object[0]);
        int nowInSec = ApolloTime.systemClockSecondsAsInt();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList(endpoints.size());
        Iterator var5 = endpoints.iterator();

        while (var5.hasNext()) {
            InetAddress endpoint = (InetAddress) var5.next();
            ValidationTask task = new ValidationTask(this.desc, endpoint, nowInSec, this.previewKind);
            tasks.add(task);
            this.session.waitForValidation(Pair.create(this.desc, endpoint), task);
            this.taskExecutor.execute(task);
        }

        return tasks;
    }

    private List<ListenableFuture<TreeResponse>> sendSequentialValidationRequest(Collection<InetAddress> endpoints) {
        String message = String.format("Requesting merkle trees for %s (to %s)", new Object[]{this.desc.columnFamily, endpoints});
        logger.info("[repair #{}] {}", this.desc.sessionId, message);
        Tracing.traceRepair(message, new Object[0]);
        int nowInSec = ApolloTime.systemClockSecondsAsInt();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList(endpoints.size());
        Queue<InetAddress> requests = new LinkedList(endpoints);
        InetAddress address = (InetAddress) requests.poll();
        ValidationTask firstTask = new ValidationTask(this.desc, address, nowInSec, this.previewKind);
        logger.info("Validating {}", address);
        this.session.waitForValidation(Pair.create(this.desc, address), firstTask);
        tasks.add(firstTask);

        for (ValidationTask currentTask = firstTask; requests.size() > 0; ) {
            final InetAddress nextAddress = (InetAddress) requests.poll();
            final ValidationTask nextTask = new ValidationTask(this.desc, nextAddress, nowInSec, this.previewKind);
            tasks.add(nextTask);
            Futures.addCallback(currentTask, new FutureCallback<TreeResponse>() {
                public void onSuccess(TreeResponse result) {
                    RepairJob.logger.info("Validating {}", nextAddress);
                    RepairJob.this.session.waitForValidation(Pair.create(RepairJob.this.desc, nextAddress), nextTask);
                    RepairJob.this.taskExecutor.execute(nextTask);
                }

                public void onFailure(Throwable t) {
                }
            });
            currentTask = nextTask;
        }

        this.taskExecutor.execute(firstTask);
        return tasks;
    }

    private List<ListenableFuture<TreeResponse>> sendDCAwareValidationRequest(Collection<InetAddress> endpoints) {
        String message = String.format("Requesting merkle trees for %s (to %s)", new Object[]{this.desc.columnFamily, endpoints});
        logger.info("[repair #{}] {}", this.desc.sessionId, message);
        Tracing.traceRepair(message, new Object[0]);
        int nowInSec = ApolloTime.systemClockSecondsAsInt();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList(endpoints.size());
        Map<String, Queue<InetAddress>> requestsByDatacenter = new HashMap();

        Queue queue;
        for (InetAddress endpoint : endpoints) {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            queue = (Queue) requestsByDatacenter.get(dc);
            if (queue == null) {
                queue = new LinkedList();
                requestsByDatacenter.put(dc, queue);
            }
            queue.add(endpoint);
        }


        for (Entry<String, Queue<InetAddress>> entry : requestsByDatacenter.entrySet()) {
            Queue<InetAddress> requests = (Queue) entry.getValue();
            InetAddress address = (InetAddress) requests.poll();
            ValidationTask firstTask = new ValidationTask(this.desc, address, nowInSec, this.previewKind);
            logger.info("Validating {}", address);
            this.session.waitForValidation(Pair.create(this.desc, address), firstTask);
            tasks.add(firstTask);

            for (ValidationTask currentTask = firstTask; requests.size() > 0; ) {
                final InetAddress nextAddress = (InetAddress) requests.poll();
                final ValidationTask nextTask = new ValidationTask(this.desc, nextAddress, nowInSec, this.previewKind);
                tasks.add(nextTask);
                Futures.addCallback(currentTask, new FutureCallback<TreeResponse>() {
                    public void onSuccess(TreeResponse result) {
                        RepairJob.logger.info("Validating {}", nextAddress);
                        RepairJob.this.session.waitForValidation(Pair.create(RepairJob.this.desc, nextAddress), nextTask);
                        RepairJob.this.taskExecutor.execute(nextTask);
                    }

                    public void onFailure(Throwable t) {
                    }
                });
                currentTask = nextTask;
            }

            this.taskExecutor.execute(firstTask);
        }

        return tasks;
    }
}
