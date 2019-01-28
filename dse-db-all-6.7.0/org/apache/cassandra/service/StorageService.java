package org.apache.cassandra.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.jmx.JMXConfiguratorMBean;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.hook.DelayingShutdownHook;
import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.bdp.db.utils.concurrent.CompletableFutures;
import com.datastax.bdp.gms.DseEndpointState;
import com.datastax.bdp.gms.DseState;
import com.datastax.bdp.server.ServerId;
import com.datastax.bdp.snitch.Workload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Completable;
import io.reactivex.functions.Action;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthSchemaChangeListener;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ParkedThreadsMonitor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PeerInfo;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SnapshotDetailsTabularData;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.StreamStateStore;
import org.apache.cassandra.dht.StreamingOptions;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.TokenSerializer;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.CallbackExpiredException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.interceptors.AbstractInterceptor;
import org.apache.cassandra.net.interceptors.InterceptionContext;
import org.apache.cassandra.net.interceptors.Interceptor;
import org.apache.cassandra.net.interceptors.MessageDirection;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.DefaultDiskErrorHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.ThreadsFactory;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.WindowsTimer;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXProgressSupport;
import org.apache.cassandra.utils.time.ApolloTime;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageService extends NotificationBroadcasterSupport implements IEndpointStateChangeSubscriber, StorageServiceMBean {
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);
    public static final int RING_DELAY = getRingDelay();
    private final JMXProgressSupport progressSupport = new JMXProgressSupport(this);
    private TokenMetadata tokenMetadata = new TokenMetadata();
    public volatile VersionedValue.VersionedValueFactory valueFactory;
    private volatile boolean bootstrapSafeToReplyEchos;
    private volatile boolean isShutdown;
    private final List<Runnable> preShutdownHooks;
    private final List<Runnable> postShutdownHooks;
    private final ConcurrentMap<InetAddress, DseEndpointState> dseEndpointStates;
    public static final StorageService instance = new StorageService();
    private final Set<InetAddress> replicatingNodes;
    private CassandraDaemon daemon;
    private InetAddress removingNode;
    private volatile boolean isBootstrapMode;
    private boolean isSurveyMode;
    private final AtomicReference<RangeStreamer> currentRebuild;
    private final AtomicBoolean isDecommissioning;
    private volatile boolean initialized;
    private volatile boolean joined;
    private volatile boolean gossipActive;
    private volatile boolean authSetupComplete;
    private double traceProbability;
    private volatile StorageService.Mode operationMode;
    private volatile int totalCFs;
    private volatile int remainingCFs;
    private static final AtomicInteger nextRepairCommand = new AtomicInteger();
    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers;
    private final ObjectName jmxObjectName;
    private Collection<Token> bootstrapTokens;
    private static final boolean useStrictConsistency = PropertyConfiguration.getBoolean("cassandra.consistent.rangemovement", true, "true when keeping strict consistency while bootstrapping");
    private static final boolean allowSimultaneousMoves = PropertyConfiguration.getBoolean("cassandra.consistent.simultaneousmoves.allow", false);
    private static final boolean joinRing;
    private boolean replacing;
    private final StreamStateStore streamStateStore;
    private final AtomicBoolean doneAuthSetup;
    public final NodeSyncService nodeSyncService;

    private static int getRingDelay() {
        String newdelay = PropertyConfiguration.PUBLIC.getString("cassandra.ring_delay_ms");
        if (newdelay != null) {
            logger.info("Overriding RING_DELAY to {}ms", newdelay);
            return Integer.parseInt(newdelay);
        } else {
            return 30000;
        }
    }

    /**
     * @deprecated
     */
    @Deprecated
    public boolean isInShutdownHook() {
        return this.isShutdown();
    }

    public boolean isShutdown() {
        return this.isShutdown;
    }

    public boolean isSafeToReplyEchos() {
        return this.initialized || this.bootstrapSafeToReplyEchos;
    }

    public Collection<Range<Token>> getLocalRanges(String keyspaceName) {
        return this.getRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getNormalizedLocalRanges(String keyspaceName) {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getNormalizedLocalRanges();
    }

    public Collection<Range<Token>> getPrimaryRanges(String keyspace) {
        return this.getPrimaryRangesForEndpoint(keyspace, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace) {
        return this.getPrimaryRangeForEndpointWithinDC(keyspace, FBUtilities.getBroadcastAddress());
    }

    public void installDiskErrorHandler() {
        JVMStabilityInspector.setDiskErrorHandler(new DefaultDiskErrorHandler(JVMStabilityInspector.killer(), this));
    }

    private void setBootstrapStateBlocking(SystemKeyspace.BootstrapState state) {
        TPCUtils.blockingAwait(SystemKeyspace.setBootstrapState(state));
    }

    private Multimap<InetAddress, Token> loadTokensBlocking() {
        return (Multimap) TPCUtils.blockingGet(SystemKeyspace.loadTokens());
    }

    private void updateTokensBlocking(InetAddress endpoint, Collection<Token> tokens) {
        TPCUtils.blockingAwait(SystemKeyspace.updateTokens(endpoint, tokens));
    }

    private void updateTokensBlocking(Collection<Token> tokens) {
        TPCUtils.blockingAwait(SystemKeyspace.updateTokens(tokens));
    }

    public void setTokens(Collection<Token> tokens) {
        assert tokens != null && !tokens.isEmpty() : "Node needs at least one token.";

        if (logger.isDebugEnabled()) {
            logger.debug("Setting tokens to {}", tokens);
        }

        this.updateTokensBlocking(tokens);
        Collection<Token> localTokens = this.getLocalTokensBlocking();
        this.setGossipTokens(localTokens);
        this.tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
        this.setMode(StorageService.Mode.NORMAL, false);
    }

    public void setGossipTokens(Collection<Token> tokens) {
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList();
        states.add(Pair.create(ApplicationState.TOKENS, this.valueFactory.tokens(tokens)));
        states.add(Pair.create(ApplicationState.STATUS, this.valueFactory.normal(tokens)));
        Gossiper.instance.addLocalApplicationStates(states);
    }

    public StorageService() {
        super(ThreadsFactory.newSingleThreadedExecutor("StorageServiceNotificationSender"));
        this.valueFactory = new VersionedValue.VersionedValueFactory(this.tokenMetadata.partitioner);
        this.bootstrapSafeToReplyEchos = false;
        this.isShutdown = false;
        this.preShutdownHooks = new ArrayList();
        this.postShutdownHooks = new ArrayList();
        this.dseEndpointStates = Maps.newConcurrentMap();
        this.replicatingNodes = Collections.synchronizedSet(SetsFactory.newSet());
        this.isSurveyMode = PropertyConfiguration.PUBLIC.getBoolean("cassandra.write_survey", false);
        this.currentRebuild = new AtomicReference();
        this.isDecommissioning = new AtomicBoolean();
        this.initialized = false;
        this.joined = false;
        this.gossipActive = false;
        this.authSetupComplete = false;
        this.traceProbability = 0.0D;
        this.operationMode = StorageService.Mode.STARTING;
        this.lifecycleSubscribers = new CopyOnWriteArrayList();
        this.bootstrapTokens = null;
        this.streamStateStore = new StreamStateStore();
        this.doneAuthSetup = new AtomicBoolean(false);
        this.nodeSyncService = new NodeSyncService();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try {
            this.jmxObjectName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            mbs.registerMBean(this, this.jmxObjectName);
            mbs.registerMBean(StreamManager.instance, new ObjectName("org.apache.cassandra.net:type=StreamManager"));
            mbs.registerMBean(MemoryOnlyStatus.instance, new ObjectName("org.apache.cassandra.db:type=MemoryOnlyStatus"));
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }

    public void registerDaemon(CassandraDaemon daemon) {
        this.daemon = daemon;
    }

    public void register(IEndpointLifecycleSubscriber subscriber) {
        this.lifecycleSubscribers.add(subscriber);
    }

    public void unregister(IEndpointLifecycleSubscriber subscriber) {
        this.lifecycleSubscribers.remove(subscriber);
    }

    public void stopGossiping() {
        this.stopGossiping("by operator request");
    }

    private void stopGossiping(String reason) {
        if (this.gossipActive) {
            logger.warn("Stopping gossip {}", reason);
            Gossiper.instance.stop();
            this.gossipActive = false;
        }

    }

    public synchronized void startGossiping() {
        if (!this.gossipActive) {
            this.checkServiceAllowedToStart("gossip");
            logger.warn("Starting gossip by operator request");
            Collection<Token> tokens = this.getSavedTokensBlocking();
            boolean validTokens = tokens != null && !tokens.isEmpty();

            assert !this.joined && !joinRing || validTokens : "Cannot start gossiping for a node intended to join without valid tokens";

            if (validTokens) {
                this.setGossipTokens(tokens);
            }

            Gossiper.instance.forceNewerGeneration();
            Gossiper.instance.start(ApolloTime.systemClockSecondsAsInt());
            Gossiper.instance.updateLocalApplicationState(ApplicationState.DSE_GOSSIP_STATE, DseState.initialLocalApplicationState());
            this.gossipActive = true;
        }

    }

    public boolean isGossipRunning() {
        return Gossiper.instance.isEnabled();
    }

    public synchronized void startNativeTransport() {
        this.checkServiceAllowedToStart("native transport");
        if (this.daemon == null) {
            throw new IllegalStateException("No configured daemon");
        } else {
            try {
                this.daemon.startNativeTransport();
            } catch (Exception var2) {
                throw new RuntimeException("Error starting native transport: " + var2.getMessage());
            }
        }
    }

    public void stopNativeTransport() {
        if (this.daemon == null) {
            throw new IllegalStateException("No configured daemon");
        } else {
            this.daemon.stopNativeTransport();
        }
    }

    public boolean isNativeTransportRunning() {
        return this.daemon == null ? false : this.daemon.isNativeTransportRunning();
    }

    public CompletableFuture stopTransportsAsync() {
        return CompletableFuture.allOf(new CompletableFuture[]{this.stopGossipingAsync(), this.stopNativeTransportAsync()});
    }

    private CompletableFuture stopGossipingAsync() {
        return !this.isGossipActive() ? TPCUtils.completedFuture() : CompletableFuture.supplyAsync(() -> {
            this.stopGossiping("by internal request (typically an unrecoverable error)");
            return null;
        }, StageManager.getStage(Stage.GOSSIP));
    }

    private CompletableFuture stopNativeTransportAsync() {
        if (!this.isNativeTransportRunning()) {
            return TPCUtils.completedFuture();
        } else {
            logger.error("Stopping native transport");
            return this.daemon.stopNativeTransportAsync();
        }
    }

    private void shutdownClientServers() {
        this.setNativeTransportReady(false);
        this.stopNativeTransport();
    }

    public void stopClient() {
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
        Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
        StageManager.shutdownNow();
    }

    public boolean isInitialized() {
        return this.initialized;
    }

    public boolean isGossipActive() {
        return this.gossipActive;
    }

    public boolean isDaemonSetupCompleted() {
        return this.daemon == null ? false : this.daemon.setupCompleted();
    }

    public void stopDaemon() {
        if (this.daemon == null) {
            throw new IllegalStateException("No configured daemon");
        } else {
            this.daemon.deactivate();
        }
    }

    @VisibleForTesting
    public CassandraDaemon getDaemon() {
        return this.daemon;
    }

    private synchronized UUID prepareForReplacement(UUID localHostId) throws ConfigurationException {
        if (SystemKeyspace.bootstrapComplete()) {
            throw new RuntimeException("Cannot replace address with a node that is already bootstrapped");
        } else if (!joinRing) {
            throw new ConfigurationException("Cannot set both join_ring=false and attempt to replace a node");
        } else if (!DatabaseDescriptor.isAutoBootstrap() && !PropertyConfiguration.getBoolean("cassandra.allow_unsafe_replace")) {
            throw new RuntimeException("Replacing a node without bootstrapping risks invalidating consistency guarantees as the expected data may not be present until repair is run. To perform this operation, please restart with -Dcassandra.allow_unsafe_replace=true");
        } else {
            InetAddress replaceAddress = DatabaseDescriptor.getReplaceAddress();
            logger.info("Gathering node replacement information for {}", replaceAddress);
            Map<InetAddress, EndpointState> epStates = Gossiper.instance.doShadowRound();
            if (epStates.get(replaceAddress) == null) {
                throw new RuntimeException(String.format("Cannot replace_address %s because it doesn't exist in gossip", new Object[]{replaceAddress}));
            }
            try {
                VersionedValue tokensVersionedValue = ((EndpointState) epStates.get(replaceAddress)).getApplicationState(ApplicationState.TOKENS);
                if (tokensVersionedValue == null) {
                    throw new RuntimeException(String.format("Could not find tokens for %s to replace", new Object[]{replaceAddress}));
                }

                this.bootstrapTokens = TokenSerializer.deserialize(this.tokenMetadata.partitioner, new DataInputStream(new ByteArrayInputStream(tokensVersionedValue.toBytes())));
            } catch (IOException var5) {
                throw new RuntimeException(var5);
            }

            if (isReplacingSameAddress()) {
                localHostId = Gossiper.instance.getHostId(replaceAddress, epStates);
                TPCUtils.blockingGet(SystemKeyspace.setLocalHostId(localHostId));
            }

            return localHostId;
        }
    }

    private synchronized void checkForEndpointCollision(UUID localHostId, Set<InetAddress> peers) throws ConfigurationException {
        if (PropertyConfiguration.getBoolean("cassandra.allow_unsafe_join")) {
            logger.warn("Skipping endpoint collision check as cassandra.allow_unsafe_join=true");
        } else {
            logger.debug("Starting shadow gossip round to check for endpoint collision");
            Map<InetAddress, EndpointState> epStates = Gossiper.instance.doShadowRound(peers);
            if (epStates.isEmpty() && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())) {
                logger.info("Unable to gossip with any peers but continuing anyway since node is in its own seed list");
            }

            if (!Gossiper.instance.isSafeForStartup(FBUtilities.getBroadcastAddress(), localHostId, this.shouldBootstrap(), epStates)) {
                throw new RuntimeException(String.format("A node with address %s already exists, cancelling join. Use cassandra.replace_address if you want to replace this node.", new Object[]{FBUtilities.getBroadcastAddress()}));
            }
            if (this.shouldBootstrap() && useStrictConsistency && !this.allowSimultaneousMoves()) {
                for (Entry<InetAddress, EndpointState> entry : epStates.entrySet()) {
                    if (!((InetAddress) entry.getKey()).equals(FBUtilities.getBroadcastAddress()) && ((EndpointState) entry.getValue()).getApplicationState(ApplicationState.STATUS) != null) {
                        String[] pieces = splitValue(((EndpointState) entry.getValue()).getApplicationState(ApplicationState.STATUS));
                        assert pieces.length > 0;

                        String state = pieces[0];
                        if (state.equals("BOOT") || state.equals("LEAVING") || state.equals("MOVING")) {
                            throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");
                        }
                    }
                }
            }

        }
    }

    private boolean allowSimultaneousMoves() {
        return allowSimultaneousMoves && DatabaseDescriptor.getNumTokens() == 1;
    }

    public void unsafeInitialize() throws ConfigurationException {
        this.initialized = true;
        this.gossipActive = true;
        Gossiper.instance.register(this);
        Gossiper.instance.start(ApolloTime.systemClockSecondsAsInt());
        Gossiper.instance.updateLocalApplicationState(ApplicationState.DSE_GOSSIP_STATE, DseState.initialLocalApplicationState());
        Gossiper.instance.updateLocalApplicationState(ApplicationState.NET_VERSION, this.valueFactory.networkVersion());
        if (!MessagingService.instance().isListening()) {
            MessagingService.instance().listen();
        }

    }

    public void populateTokenMetadata() {
        if (PropertyConfiguration.PUBLIC.getBoolean("cassandra.load_ring_state", true)) {
            logger.info("Populating token metadata from system tables");
            Multimap<InetAddress, Token> loadedTokens = this.loadTokensBlocking();
            if (!this.shouldBootstrap()) {
                loadedTokens.putAll(FBUtilities.getBroadcastAddress(), this.getSavedTokensBlocking());
            }
            for (InetAddress ep : loadedTokens.keySet()) {
                this.tokenMetadata.updateNormalTokens(loadedTokens.get(ep), ep);
            }
            logger.info("Token metadata: {}", this.tokenMetadata);
        }

    }

    public synchronized void initServer() throws ConfigurationException {
        this.initServer(RING_DELAY);
    }

    public synchronized void initServer(int delay) throws ConfigurationException {
        SystemKeyspace.finishStartupBlocking();
        logger.info("DSE version: {}", ProductVersion.getDSEFullVersionString());
        if (ProductVersion.getDSEFullVersion().snapshot) {
            logger.info("Further information DSE snapshot build: git sha {}, branch {}", ProductVersion.getProductVersionString("vcs_id"), ProductVersion.getProductVersionString("vcs_branch"));
        }

        logger.info("CQL supported versions: {} (default: {})", StringUtils.join(ClientState.getCQLSupportedVersion(), ", "), ClientState.DEFAULT_CQL_VERSION);
        logger.info("Native protocol supported versions: {} (default: {})", StringUtils.join(ProtocolVersion.supportedVersions(), ", "), ProtocolVersion.CURRENT);

        try {
            Class.forName("org.apache.cassandra.service.StorageProxy");
        } catch (ClassNotFoundException var6) {
            throw new AssertionError(var6);
        }

        Thread drainOnShutdown = NamedThreadFactory.createThread(new WrappedRunnable() {
            public void runMayThrow() throws InterruptedException, ExecutionException, IOException {
                StorageService.this.drain(true);
                if (FBUtilities.isWindows) {
                    WindowsTimer.endTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
                }

                DelayingShutdownHook logbackHook = new DelayingShutdownHook();
                logbackHook.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
                logbackHook.run();
            }
        }, "StorageServiceShutdownHook");
        JVMStabilityInspector.registerShutdownHook(drainOnShutdown, this::onShutdownHookRemoved);
        this.replacing = this.isReplacing();
        if (!PropertyConfiguration.getBoolean("cassandra.start_gossip", true)) {
            logger.info("Not starting gossip as requested.");
            this.loadRingState();
            this.initialized = true;
        } else {
            this.prepareToJoin();

            try {
                CacheService.instance.counterCache.loadSavedAsync().get();
            } catch (Throwable var5) {
                JVMStabilityInspector.inspectThrowable(var5);
                logger.warn("Error loading counter cache", var5);
            }

            if (joinRing) {
                this.joinTokenRing(delay);
            } else {
                Collection<Token> tokens = this.getSavedTokensBlocking();
                if (!tokens.isEmpty()) {
                    this.tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
                    List<Pair<ApplicationState, VersionedValue>> states = new ArrayList();
                    states.add(Pair.create(ApplicationState.TOKENS, this.valueFactory.tokens(tokens)));
                    states.add(Pair.create(ApplicationState.STATUS, this.valueFactory.hibernate(true)));
                    Gossiper.instance.addLocalApplicationStates(states);
                }

                TPCUtils.blockingAwait(this.doAuthSetup());
                TPCUtils.blockingAwait(this.doAuditLoggingSetup());
                logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
            }

            logger.info("Snitch information: {}, local DC:{} / rack:{}", new Object[]{DatabaseDescriptor.getEndpointSnitch(), DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter(), DatabaseDescriptor.getEndpointSnitch().getLocalRack()});
            this.initialized = true;
        }
    }


    private void primeConnections() {
        Set<InetAddress> liveRingMembers = Collections.emptySet();
        int MAX_PRIMING_ATTEMPTS = PropertyConfiguration.getInteger("cassandra.max_gossip_priming_attempts", 3);
        String key = "NULL";
        String longKey = Strings.repeat((String) key, (int) ((int) OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD / key.length() + 1));
        longKey = longKey.substring(0, (int) OutboundTcpConnectionPool.LARGE_MESSAGE_THRESHOLD - 1);
        TableMetadata cf = SystemKeyspace.metadata().tables.getNullable("local");
        DecoratedKey dKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.wrap(key.getBytes()));
        DecoratedKey dLongKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.wrap(longKey.getBytes()));
        for (int i = 0; i < MAX_PRIMING_ATTEMPTS; ++i) {
            liveRingMembers = this.getLiveRingMembers();
            SinglePartitionReadCommand qs = SinglePartitionReadCommand.create(cf, ApolloTime.systemClockSecondsAsInt(), dKey, ColumnFilter.selection(RegularAndStaticColumns.NONE), new ClusteringIndexSliceFilter(Slices.ALL, false));
            SinglePartitionReadCommand ql = SinglePartitionReadCommand.create(cf, ApolloTime.systemClockSecondsAsInt(), dLongKey, ColumnFilter.selection(RegularAndStaticColumns.NONE), new ClusteringIndexSliceFilter(Slices.ALL, false));
            HashMultimap<InetAddress, CompletableFuture> responses = HashMultimap.create();
            for (InetAddress ep : liveRingMembers) {
                if (ep.equals(FBUtilities.getBroadcastAddress())) continue;
                OutboundTcpConnectionPool pool = MessagingService.instance().getConnectionPool((InetAddress) ep).join();
                try {
                    pool.waitForStarted();
                } catch (IllegalStateException e) {
                    logger.warn("Outgoing Connection pool failed to start for {}", ep);
                    continue;
                }
                responses.put(ep, MessagingService.instance().sendSingleTarget(Verbs.READS.SINGLE_READ.newRequest((InetAddress) ep, qs)));
                responses.put(ep, MessagingService.instance().sendSingleTarget(Verbs.READS.SINGLE_READ.newRequest((InetAddress) ep, ql)));
            }
            try {
                FBUtilities.waitOnFutures(Lists.newArrayList((Iterable) responses.values()), DatabaseDescriptor.getReadRpcTimeout());
            } catch (Throwable tm) {
                for (Entry<InetAddress, CompletableFuture> entry : responses.entries()) {
                    if (((CompletableFuture) entry.getValue()).isCompletedExceptionally()) continue;
                    logger.debug("Timeout waiting for priming response from {}", entry.getKey());
                }
                continue;
            }
            logger.debug("All priming requests succeeded");
            break;
        }
        for (InetAddress ep : liveRingMembers) {
            if (ep.equals(FBUtilities.getBroadcastAddress())) continue;
            OutboundTcpConnectionPool pool = MessagingService.instance().getConnectionPool(ep).join();
            if (!pool.gossip().isSocketOpen()) {
                logger.warn("Gossip connection to {} not open", (Object) ep);
            }
            if (!pool.small().isSocketOpen()) {
                logger.warn("Small message connection to {} not open", (Object) ep);
            }
            if (pool.large().isSocketOpen()) continue;
            logger.warn("Large message connection to {} not open", (Object) ep);
        }
    }

    private void loadRingState() {
        if (PropertyConfiguration.PUBLIC.getBoolean("cassandra.load_ring_state", true)) {
            logger.info("Loading persisted ring state");
            Multimap<InetAddress, Token> loadedTokens = this.loadTokensBlocking();
            Map<InetAddress, UUID> loadedHostIds = SystemKeyspace.getHostIds();

            for (InetAddress ep : loadedTokens.keySet()) {
                if (ep.equals(FBUtilities.getBroadcastAddress())) {
                    this.removeEndpointBlocking(ep, false);
                } else {
                    if (loadedHostIds.containsKey(ep)) {
                        this.tokenMetadata.updateHostId((UUID) loadedHostIds.get(ep), ep);
                    }
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }

    }

    public boolean isReplacing() {
        if (PropertyConfiguration.getString("cassandra.replace_address_first_boot", (String) null) != null && SystemKeyspace.bootstrapComplete()) {
            logger.info("Replace address on first boot requested; this node is already bootstrapped");
            return false;
        } else {
            return DatabaseDescriptor.getReplaceAddress() != null;
        }
    }

    public void onShutdownHookRemoved() {
        if (FBUtilities.isWindows) {
            WindowsTimer.endTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
        }

    }

    private boolean shouldBootstrap() {
        return DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && !isSeed();
    }

    public static boolean isSeed() {
        return DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress());
    }

    private void prepareToJoin() throws ConfigurationException {
        if (!this.joined) {
            Map<ApplicationState, VersionedValue> appStates = new EnumMap(ApplicationState.class);
            if (SystemKeyspace.wasDecommissioned()) {
                if (!PropertyConfiguration.getBoolean("cassandra.override_decommission")) {
                    throw new ConfigurationException("This node was decommissioned and will not rejoin the ring unless cassandra.override_decommission=true has been set, or all existing data is removed and the node is bootstrapped again");
                }

                logger.warn("This node was decommissioned, but overriding by operator request.");
                this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.COMPLETED);
            }

            if (DatabaseDescriptor.getReplaceTokens().size() > 0 || DatabaseDescriptor.getReplaceNode() != null) {
                throw new RuntimeException("Replace method removed; use cassandra.replace_address instead");
            }

            Interceptor gossiperInitGuard = newGossiperInitGuard();
            MessagingService.instance().addInterceptor(gossiperInitGuard);
            if (!MessagingService.instance().isListening()) {
                MessagingService.instance().listen();
            }

            UUID localHostId = (UUID) TPCUtils.blockingGet(SystemKeyspace.setLocalHostId());
            if (this.replacing) {
                localHostId = this.prepareForReplacement(localHostId);
                appStates.put(ApplicationState.TOKENS, this.valueFactory.tokens(this.bootstrapTokens));
                if (!DatabaseDescriptor.isAutoBootstrap()) {
                    this.updateTokensBlocking(this.bootstrapTokens);
                } else if (isReplacingSameAddress()) {
                    logger.warn("Writes will not be forwarded to this node during replacement because it has the same address as the node to be replaced ({}). If the previous node has been down for longer than max_hint_window_in_ms, repair must be run after the replacement process in order to make this node consistent.", DatabaseDescriptor.getReplaceAddress());
                    appStates.put(ApplicationState.STATUS, this.valueFactory.hibernate(true));
                }
            } else {
                this.checkForEndpointCollision(localHostId, SystemKeyspace.getHostIds().keySet());
            }

            this.getTokenMetadata().updateHostId(localHostId, FBUtilities.getBroadcastAddress());
            appStates.put(ApplicationState.NET_VERSION, this.valueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, this.valueFactory.hostId(localHostId));
            appStates.put(ApplicationState.NATIVE_TRANSPORT_ADDRESS, this.valueFactory.rpcaddress(FBUtilities.getNativeTransportBroadcastAddress()));
            appStates.put(ApplicationState.RELEASE_VERSION, this.valueFactory.releaseVersion());
            appStates.put(ApplicationState.SCHEMA_COMPATIBILITY_VERSION, this.valueFactory.schemaCompatibilityVersion());
            if (!this.shouldBootstrap()) {
                appStates.put(ApplicationState.STATUS, this.valueFactory.hibernate(true));
            }

            appStates.put(ApplicationState.NATIVE_TRANSPORT_PORT, this.valueFactory.nativeTransportPort(DatabaseDescriptor.getNativeTransportPort()));
            appStates.put(ApplicationState.NATIVE_TRANSPORT_PORT_SSL, this.valueFactory.nativeTransportPortSSL(DatabaseDescriptor.getNativeTransportPortSSL()));
            appStates.put(ApplicationState.STORAGE_PORT, this.valueFactory.storagePort(DatabaseDescriptor.getStoragePort()));
            appStates.put(ApplicationState.STORAGE_PORT_SSL, this.valueFactory.storagePortSSL(DatabaseDescriptor.getSSLStoragePort()));
            DatabaseDescriptor.getJMXPort().ifPresent((port) -> {
                appStates.put(ApplicationState.JMX_PORT, this.valueFactory.jmxPort(port.intValue()));
            });
            appStates.put(ApplicationState.DSE_GOSSIP_STATE, DseState.initialLocalApplicationState());
            this.loadRingState();
            logger.info("Starting up server gossip");
            Gossiper.instance.register(this);
            int generation = ((Integer) TPCUtils.blockingGet(SystemKeyspace.incrementAndGetGeneration())).intValue();
            Gossiper.instance.start(generation, appStates);
            this.gossipActive = true;
            if (this.shouldBootstrap()) {
                this.bootstrapSafeToReplyEchos = true;
            }

            MessagingService.instance().removeInterceptor(gossiperInitGuard);
            this.gossipSnitchInfo();
            Schema.instance.updateVersionAndAnnounce();
            LoadBroadcaster.instance.startBroadcasting();
            HintsService.instance.startDispatch();
            Gossiper.waitToSettle("accepting client requests");
            BatchlogManager.instance.start();
        }

    }

    public void waitForSchema(int delay) {
        logger.debug("Waiting for schema (max {} seconds)", Integer.valueOf(delay));

        for (int i = 0; i < delay; i += 1000) {
            if (!Schema.instance.isEmpty()) {
                logger.debug("current schema version: {}", Schema.instance.getVersion());
                break;
            }

            Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
        }

        if (!MigrationManager.isReadyForBootstrap()) {
            this.setMode(StorageService.Mode.JOINING, "waiting for schema information to complete", true);
            MigrationManager.waitUntilReadyForBootstrap();
        }

        logger.info("Has schema with version {}", Schema.instance.getVersion());
    }

    private void joinTokenRing(int delay) throws ConfigurationException {
        this.joined = true;
        long nanoTimeNodeUpdatedOffset = ApolloTime.approximateNanoTime();
        Set<InetAddress> current = SetsFactory.newSet();
        if (logger.isDebugEnabled()) {
            logger.debug("Bootstrap variables: {} {} {} {}", new Object[]{DatabaseDescriptor.isAutoBootstrap(), SystemKeyspace.bootstrapInProgress(), SystemKeyspace.bootstrapComplete(), DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())});
        }
        if (DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress())) {
            logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
        }
        boolean dataAvailable = true;
        boolean bootstrap = this.shouldBootstrap();
        if (bootstrap) {
            if (SystemKeyspace.bootstrapInProgress()) {
                logger.warn("Detected previous bootstrap failure; retrying");
            } else {
                this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.IN_PROGRESS);
            }
            this.setMode(Mode.JOINING, "waiting for ring information", true);
            this.waitForSchema(delay);
            this.setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);
            this.setMode(Mode.JOINING, "waiting for pending range calculation", true);
            PendingRangeCalculatorService.instance.blockUntilFinished();
            this.setMode(Mode.JOINING, "calculation complete, ready to bootstrap", true);
            logger.debug("... got ring + schema info ({})", (Object) Schema.instance.getVersion());
            if (useStrictConsistency && !this.allowSimultaneousMoves() && (this.tokenMetadata.getBootstrapTokens().valueSet().size() > 0 || this.tokenMetadata.getSizeOfLeavingEndpoints() > 0 || this.tokenMetadata.getSizeOfMovingEndpoints() > 0)) {
                String bootstrapTokens = StringUtils.join(this.tokenMetadata.getBootstrapTokens().valueSet(), (char) ',');
                String leavingTokens = StringUtils.join(this.tokenMetadata.getLeavingEndpoints(), (char) ',');
                String movingTokens = StringUtils.join((Object[]) this.tokenMetadata.getMovingEndpoints().stream().map(e -> (InetAddress) e.right).toArray(), (char) ',');
                throw new UnsupportedOperationException(String.format("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true. Nodes detected, bootstrapping: %s; leaving: %s; moving: %s;", bootstrapTokens, leavingTokens, movingTokens));
            }
            if (!this.replacing) {
                if (this.tokenMetadata.isMember(FBUtilities.getBroadcastAddress())) {
                    String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                this.setMode(Mode.JOINING, "getting bootstrap token", true);
                this.bootstrapTokens = BootStrapper.getBootstrapTokens(this.tokenMetadata, FBUtilities.getBroadcastAddress(), delay);
            } else {
                if (!StorageService.isReplacingSameAddress()) {
                    String FAILED_REPLACE_MSG = String.format("If this node failed replace recently, wait at least %ds before starting a new replace operation.", TimeUnit.MILLISECONDS.toSeconds(Gossiper.QUARANTINE_DELAY));
                    Set<InetAddress> previousAddresses = SetsFactory.newSet();
                    for (Token token : this.bootstrapTokens) {
                        InetAddress existing = this.tokenMetadata.getEndpoint(token);
                        if (existing == null) {
                            throw new UnsupportedOperationException("Cannot replace token " + token + " which does not exist! " + (String) FAILED_REPLACE_MSG);
                        }
                        previousAddresses.add(existing);
                    }
                    long timeSleepOffset = ApolloTime.systemClockMillis();
                    long timeEnd = timeSleepOffset + (long) delay + (long) Math.min(LoadBroadcaster.BROADCAST_INTERVAL, 1000);
                    do {
                        Uninterruptibles.sleepUninterruptibly((long) 1L, (TimeUnit) TimeUnit.SECONDS);
                        for (InetAddress existing : previousAddresses) {
                            long updateTimestamp = Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp();
                            if (nanoTimeNodeUpdatedOffset - updateTimestamp >= 0L) continue;
                            logger.error("Cannot replace a live node {}. Endpoint state changed since {} (nanotime={}). {}", new Object[]{existing, new Date(timeSleepOffset), updateTimestamp, FAILED_REPLACE_MSG});
                            throw new UnsupportedOperationException("Cannot replace a live node... " + (String) FAILED_REPLACE_MSG);
                        }
                    } while (ApolloTime.systemClockMillis() - timeEnd < 0L);
                    current.addAll(previousAddresses);
                } else {
                    Uninterruptibles.sleepUninterruptibly((long) RING_DELAY, (TimeUnit) TimeUnit.MILLISECONDS);
                }
                this.setMode(Mode.JOINING, "Replacing a node with token(s): " + this.bootstrapTokens, true);
            }
            dataAvailable = this.bootstrap(this.bootstrapTokens);
        } else {
            this.bootstrapTokens = this.getSavedTokensBlocking();
            if (this.bootstrapTokens.isEmpty()) {
                this.bootstrapTokens = BootStrapper.getBootstrapTokens(this.tokenMetadata, FBUtilities.getBroadcastAddress(), delay);
            } else {
                if (this.bootstrapTokens.size() != DatabaseDescriptor.getNumTokens()) {
                    throw new ConfigurationException("Cannot change the number of tokens from " + this.bootstrapTokens.size() + " to " + DatabaseDescriptor.getNumTokens());
                }
                logger.info("Using saved tokens {}", this.bootstrapTokens);
            }
        }
        this.maybeAddOrUpdateKeyspace(TraceKeyspace.metadata()).andThen(this.maybeAddOrUpdateKeyspace(SystemDistributedKeyspace.metadata())).blockingAwait();
        if (!this.isSurveyMode) {
            if (dataAvailable) {
                this.finishJoiningRing(bootstrap, this.bootstrapTokens);
                if (!current.isEmpty()) {
                    for (InetAddress existing : current) {
                        Gossiper.instance.replacedEndpoint(existing);
                    }
                }
                logger.info("Startup with data available + schema info ({})", (Object) Schema.instance.getVersion());
            } else {
                logger.warn("Some data streaming failed. Use nodetool to check bootstrap state and resume. For more, see `nodetool help bootstrap`. {}", (Object) SystemKeyspace.getBootstrapState());
                TPCUtils.blockingAwait(this.doAuthSetup());
            }
        } else {
            logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
            TPCUtils.blockingAwait(this.doAuthSetup());
        }
    }

    public static boolean isReplacingSameAddress() {
        InetAddress replaceAddress = DatabaseDescriptor.getReplaceAddress();
        return replaceAddress != null && replaceAddress.equals(FBUtilities.getBroadcastAddress());
    }

    public static BootStrapper.StreamConsistency getReplaceConsistency() {
        try {
            return BootStrapper.StreamConsistency.valueOf(PropertyConfiguration.getString("dse.consistent_replace", "ONE").toUpperCase());
        } catch (IllegalArgumentException var1) {
            logger.warn("Could not parse -Ddse.consistent_replace={} property. Using replace consistency of ONE.", System.getProperty("dse.consistent_replace"));
            return BootStrapper.StreamConsistency.ONE;
        }
    }

    public void gossipSnitchInfo() {
        String dc = DatabaseDescriptor.getLocalDataCenter();
        String rack = DatabaseDescriptor.getLocalRack();
        Gossiper.instance.updateLocalApplicationState(ApplicationState.DC, instance.valueFactory.datacenter(dc));
        Gossiper.instance.updateLocalApplicationState(ApplicationState.RACK, instance.valueFactory.rack(rack));
    }

    public void joinRing() throws IOException {
        SystemKeyspace.BootstrapState state = SystemKeyspace.getBootstrapState();
        this.joinRing(state.equals(SystemKeyspace.BootstrapState.IN_PROGRESS));
    }

    private synchronized void joinRing(boolean resumedBootstrap) throws IOException {
        if (!this.joined) {
            logger.info("Joining ring by operator request");

            try {
                this.joinTokenRing(0);
            } catch (ConfigurationException var3) {
                throw new IOException(var3.getMessage());
            }
        } else if (this.isSurveyMode) {
            logger.info("Leaving write survey mode and joining ring at operator request");
            this.finishJoiningRing(resumedBootstrap, this.getSavedTokensBlocking());
            this.isSurveyMode = false;
        }

    }

    private void executePreJoinTasks(boolean bootstrap) {
        StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false).filter((cfs) -> {
            return Schema.instance.getUserKeyspaces().contains(cfs.keyspace.getName());
        }).forEach((cfs) -> {
            cfs.indexManager.executePreJoinTasksBlocking(bootstrap);
        });
    }

    private void finishJoiningRing(boolean didBootstrap, Collection<Token> tokens) {
        this.primeConnections();
        this.setMode(StorageService.Mode.JOINING, "Finish joining ring", true);
        this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.COMPLETED);
        this.executePreJoinTasks(didBootstrap);
        this.setTokens(tokens);

        assert this.tokenMetadata.sortedTokens().size() > 0;

        TPCUtils.blockingAwait(this.doAuthSetup());
        TPCUtils.blockingAwait(this.doAuditLoggingSetup());
    }

    private Completable doAuthSetup() {
        return this.doneAuthSetup.getAndSet(true) ? Completable.complete() : this.maybeAddOrUpdateKeyspace(AuthKeyspace.metadata(), AuthKeyspace.tablesIfNotExist(), 0L).doOnComplete(() -> {
            DatabaseDescriptor.getRoleManager().setup();
            DatabaseDescriptor.getAuthenticator().setup();
            DatabaseDescriptor.getAuthorizer().setup();
            Schema.instance.registerListener(new AuthSchemaChangeListener());
            this.authSetupComplete = true;
        });
    }

    public boolean isAuthSetupComplete() {
        return this.authSetupComplete;
    }

    private Completable doAuditLoggingSetup() {
        DatabaseDescriptor.getAuditLogger().setup();
        return Completable.complete();
    }

    private Completable maybeAddKeyspace(KeyspaceMetadata ksm) {
        return MigrationManager.announceNewKeyspace(ksm, 0L, false).onErrorResumeNext((e) -> {
            if (e instanceof AlreadyExistsException) {
                logger.debug("Attempted to create new keyspace {}, but it already exists", ksm.name);
                return Completable.complete();
            } else {
                return Completable.error(e);
            }
        });
    }

    private Completable maybeAddOrUpdateKeyspace(KeyspaceMetadata expected) {
        return this.maybeAddOrUpdateKeyspace(expected, UnmodifiableArrayList.emptyList(), 0L);
    }


    public Completable maybeAddOrUpdateKeyspace(KeyspaceMetadata expected, List<TableMetadata> tablesIfNotExist, long timestamp) {
        Completable migration = Schema.instance.getKeyspaceMetadata(expected.name) == null ? this.maybeAddKeyspace(expected) : Completable.complete();
        return migration.andThen(Completable.defer(() -> {
            KeyspaceMetadata defined = Schema.instance.getKeyspaceMetadata(expected.name);
            return this.maybeAddOrUpdateTypes(expected.types, defined.types, timestamp);
        })).andThen(Completable.defer(() -> {
            KeyspaceMetadata defined = Schema.instance.getKeyspaceMetadata(expected.name);
            Tables expectedTables = expected.tables;
            for (TableMetadata tableIfNotExists : tablesIfNotExist) {
                if (defined.tables.getNullable(tableIfNotExists.name) != null) continue;
                expectedTables = expectedTables.with(tableIfNotExists);
            }
            return this.maybeAddOrUpdateTables(expectedTables, defined.tables, timestamp);
        }));
    }

    private Completable maybeAddOrUpdateTypes(final Types expected, final Types defined, final long timestamp) {
        final List<Completable> migrations = new ArrayList<Completable>();
        for (final UserType expectedType : expected) {
            final UserType definedType = defined.get(expectedType.name).orElse(null);
            if (definedType == null || !definedType.equals(expectedType)) {
                migrations.add(MigrationManager.forceAnnounceNewType(expectedType, timestamp));
            }
        }
        return migrations.isEmpty() ? Completable.complete() : Completable.merge((Iterable)migrations);
    }

    private Completable maybeAddOrUpdateTables(Tables expected, Tables defined, long timestamp) {
        List<Completable> migrations = new ArrayList();
        for (TableMetadata expectedTable : expected) {
            TableMetadata definedTable = (TableMetadata) defined.get(expectedTable.name).orElse(null);
            if (definedTable == null) {
                migrations.add(MigrationManager.forceAnnounceNewTable(expectedTable, timestamp));
            } else if (!definedTable.equalsIgnoringNodeSync(expectedTable)) {
                TableParams newParams = expectedTable.params.unbuild().nodeSync(definedTable.params.nodeSync).build();
                migrations.add(MigrationManager.forceAnnounceNewTable(expectedTable.unbuild().params(newParams).build(), timestamp));
            }
        }

        return migrations.isEmpty() ? Completable.complete() : Completable.concat(migrations);
    }

    public boolean isJoined() {
        return this.joined && !this.isSurveyMode;
    }

    public void rebuild(String sourceDc) {
        this.rebuild(sourceDc, (String) null, (String) null, (String) null);
    }

    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources) {
        this.rebuild(keyspace != null ? UnmodifiableArrayList.of(keyspace) : UnmodifiableArrayList.emptyList(), tokens, RebuildMode.NORMAL, 0, StreamingOptions.forRebuild(this.tokenMetadata.cloneOnlyTokenMap(), sourceDc, specificSources));
    }

    public String rebuild(List<String> keyspaces, String tokens, String mode, List<String> srcDcNames, List<String> excludeDcNames, List<String> specifiedSources, List<String> excludeSources) {
        return this.rebuild(keyspaces, tokens, mode, 0, srcDcNames, excludeDcNames, specifiedSources, excludeSources);
    }

    public String rebuild(List<String> keyspaces, String tokens, String mode, int streamingConnectionsPerHost, List<String> srcDcNames, List<String> excludeDcNames, List<String> specifiedSources, List<String> excludeSources) {
        return this.rebuild((List) (keyspaces != null ? keyspaces : UnmodifiableArrayList.emptyList()), tokens, RebuildMode.getMode(mode), streamingConnectionsPerHost, StreamingOptions.forRebuild(this.tokenMetadata.cloneOnlyTokenMap(), srcDcNames, excludeDcNames, specifiedSources, excludeSources));
    }


    public List<String> getLocallyReplicatedKeyspaces() {
        return Schema.instance.getKeyspaces().stream().map(Schema.instance::getKeyspaceInstance).filter(ks -> ks.getReplicationStrategy().getClass() != LocalStrategy.class).filter(ks -> ks.getReplicationStrategy().isReplicatedInDatacenter(DatabaseDescriptor.getLocalDataCenter())).map(Keyspace::getName).sorted().collect(Collectors.toList());
    }


    private String rebuild(List<String> keyspaces, final String tokens, final RebuildMode mode, int streamingConnectionsPerHost, final StreamingOptions options) {
        keyspaces = (List<String>) ((keyspaces != null) ? keyspaces : UnmodifiableArrayList.emptyList());
        if (keyspaces.isEmpty() && tokens != null) {
            throw new IllegalArgumentException("Cannot specify tokens without keyspace.");
        }
        final List<String> keyspaceValidationErrors = new ArrayList<String>();
        for (final String keyspace : keyspaces) {
            if (SchemaConstants.isLocalSystemKeyspace(keyspace)) {
                keyspaceValidationErrors.add(String.format("Keyspace '%s' is a local system keyspace and must not be used for rebuild", keyspace));
            }
            final Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
            if (ks.getReplicationStrategy().getClass() == LocalStrategy.class) {
                keyspaceValidationErrors.add(String.format("Keyspace '%s' uses LocalStrategy and must not be used for rebuild", keyspace));
            }
            if (!ks.getReplicationStrategy().isReplicatedInDatacenter(DatabaseDescriptor.getLocalDataCenter())) {
                keyspaceValidationErrors.add(String.format("Keyspace '%s' is not replicated locally and must not be used for rebuild", keyspace));
            }
        }
        if (!keyspaceValidationErrors.isEmpty()) {
            StorageService.logger.error("Rejected rebuild for keyspaces '{}' due to {}", (Object) String.join(", ", keyspaces), (Object) String.join(", ", keyspaceValidationErrors));
            throw new IllegalArgumentException(String.join(", ", keyspaceValidationErrors));
        }
        if (streamingConnectionsPerHost <= 0) {
            streamingConnectionsPerHost = DatabaseDescriptor.getStreamingConnectionsPerHost();
        }
        final String msg = String.format("%s, %s, %d streaming connections, %s, %s", keyspaces.isEmpty() ? "(All keyspaces)" : keyspaces, (tokens == null) ? "(All tokens)" : tokens, streamingConnectionsPerHost, mode, options);
        StorageService.logger.info("starting rebuild for {}", (Object) msg);
        final long t0 = ApolloTime.millisSinceStartup();
        final RangeStreamer streamer = new RangeStreamer(this.tokenMetadata, null, FBUtilities.getBroadcastAddress(), StreamOperation.REBUILD, StorageService.useStrictConsistency && !this.replacing, DatabaseDescriptor.getEndpointSnitch(), this.streamStateStore, false, streamingConnectionsPerHost, options.toSourceFilter(DatabaseDescriptor.getEndpointSnitch(), FailureDetector.instance));
        if (!this.currentRebuild.compareAndSet(null, streamer)) {
            throw new IllegalStateException("Node is still rebuilding. Check nodetool netstats.");
        }
        try {
            if (keyspaces.isEmpty()) {
                keyspaces = this.getLocallyReplicatedKeyspaces();
            }
            if (tokens == null) {
                for (final String keyspaceName : keyspaces) {
                    streamer.addRanges(keyspaceName, this.getLocalRanges(keyspaceName));
                }
                mode.beforeStreaming(keyspaces);
            } else {
                final List<Range<Token>> ranges = new ArrayList<Range<Token>>();
                final Token.TokenFactory factory = this.getTokenFactory();
                final Pattern rangePattern = Pattern.compile("\\(\\s*(-?\\w+)\\s*,\\s*(-?\\w+)\\s*\\]");
                try (final Scanner tokenScanner = new Scanner(tokens)) {
                    while (tokenScanner.findInLine(rangePattern) != null) {
                        final MatchResult range = tokenScanner.match();
                        final Token startToken = factory.fromString(range.group(1));
                        final Token endToken = factory.fromString(range.group(2));
                        StorageService.logger.info("adding range: ({},{}]", (Object) startToken, (Object) endToken);
                        ranges.add(new Range<Token>(startToken, endToken));
                    }
                    if (tokenScanner.hasNext()) {
                        throw new IllegalArgumentException("Unexpected string: " + tokenScanner.next());
                    }
                }
                final Map<String, Collection<Range<Token>>> keyspaceRanges = new HashMap<String, Collection<Range<Token>>>();
                for (final String keyspaceName2 : keyspaces) {
                    final Collection<Range<Token>> localRanges = this.getLocalRanges(keyspaceName2);
                    final Set<Range<Token>> specifiedNotFoundRanges = SetsFactory.setFromCollection(ranges);
                    for (final Range<Token> specifiedRange : ranges) {
                        for (final Range<Token> localRange : localRanges) {
                            if (localRange.contains(specifiedRange)) {
                                specifiedNotFoundRanges.remove(specifiedRange);
                                break;
                            }
                        }
                    }
                    if (!specifiedNotFoundRanges.isEmpty()) {
                        throw new IllegalArgumentException(String.format("The specified range(s) %s is not a range that is owned by this node. Please ensure that all token ranges specified to be rebuilt belong to this node.", specifiedNotFoundRanges));
                    }
                    streamer.addRanges(keyspaceName2, ranges);
                    keyspaceRanges.put(keyspaceName2, ranges);
                }
                mode.beforeStreaming(keyspaceRanges);
            }
            final ListenableFuture<StreamState> resultFuture = streamer.fetchAsync(null);
            final StreamState streamState = (StreamState) resultFuture.get();
            final long t2 = ApolloTime.millisSinceStartupDelta(t0);
            long totalBytes = 0L;
            for (final SessionInfo session : streamState.sessions) {
                totalBytes += session.getTotalSizeReceived();
            }
            final String info = String.format("finished rebuild for %s after %d seconds receiving %s.", msg, t2 / 1000L, FileUtils.stringifyFileSize(totalBytes));
            StorageService.logger.info("{}", (Object) info);
            return info;
        } catch (InterruptedException e3) {
            throw new RuntimeException("Interrupted while waiting on rebuild streaming");
        } catch (IllegalArgumentException | IllegalStateException ex2) {
            final RuntimeException e = ex2;
            StorageService.logger.warn("Parameter error while rebuilding node", (Throwable) e);
            throw new RuntimeException("Parameter error while rebuilding node: " + e);
        } catch (ExecutionException e2) {
            StorageService.logger.error("Error while rebuilding node", e2.getCause());
            throw new RuntimeException("Error while rebuilding node: " + e2.getCause().getMessage());
        } catch (RuntimeException e) {
            StorageService.logger.error("Error while rebuilding node", (Throwable) e);
            throw e;
        } finally {
            this.currentRebuild.set(null);
        }
    }

    public void setRpcTimeout(long value) {
        DatabaseDescriptor.setRpcTimeout(value);
        logger.info("set rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getRpcTimeout() {
        return DatabaseDescriptor.getRpcTimeout();
    }

    public void setReadRpcTimeout(long value) {
        DatabaseDescriptor.setReadRpcTimeout(value);
        logger.info("set read rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getReadRpcTimeout() {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public void setRangeRpcTimeout(long value) {
        DatabaseDescriptor.setRangeRpcTimeout(value);
        logger.info("set range rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getRangeRpcTimeout() {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    public void setWriteRpcTimeout(long value) {
        DatabaseDescriptor.setWriteRpcTimeout(value);
        logger.info("set write rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getWriteRpcTimeout() {
        return DatabaseDescriptor.getWriteRpcTimeout();
    }

    public void setCounterWriteRpcTimeout(long value) {
        DatabaseDescriptor.setCounterWriteRpcTimeout(value);
        logger.info("set counter write rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getCounterWriteRpcTimeout() {
        return DatabaseDescriptor.getCounterWriteRpcTimeout();
    }

    public void setCasContentionTimeout(long value) {
        DatabaseDescriptor.setCasContentionTimeout(value);
        logger.info("set cas contention rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getCasContentionTimeout() {
        return DatabaseDescriptor.getCasContentionTimeout();
    }

    public void setTruncateRpcTimeout(long value) {
        DatabaseDescriptor.setTruncateRpcTimeout(value);
        logger.info("set truncate rpc timeout to {} ms", Long.valueOf(value));
    }

    public long getTruncateRpcTimeout() {
        return DatabaseDescriptor.getTruncateRpcTimeout();
    }

    public void abortRebuild(String reason) {
        if (reason == null) {
            reason = "Manually aborted";
        }

        RangeStreamer streamer = (RangeStreamer) this.currentRebuild.get();
        if (streamer == null) {
            throw new IllegalStateException("No active rebuild");
        } else {
            streamer.abort(reason);
        }
    }

    public void setStreamThroughputMbPerSec(int value) {
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(value);
        logger.info("setstreamthroughput: throttle set to {}", Integer.valueOf(value));
    }

    public int getStreamThroughputMbPerSec() {
        return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
    }

    public void setStreamingConnectionsPerHost(int value) {
        DatabaseDescriptor.setStreamingConnectionsPerHost(value);
    }

    public int getStreamingConnectionsPerHost() {
        return DatabaseDescriptor.getStreamingConnectionsPerHost();
    }

    public void setInterDCStreamThroughputMbPerSec(int value) {
        DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(value);
        logger.info("setinterdcstreamthroughput: throttle set to {}", Integer.valueOf(value));
    }

    public int getInterDCStreamThroughputMbPerSec() {
        return DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec();
    }

    public int getCompactionThroughputMbPerSec() {
        return DatabaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value) {
        DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
        CompactionManager.instance.setRate((double) value);
    }

    public int getBatchlogReplayThrottleInKB() {
        return DatabaseDescriptor.getBatchlogReplayThrottleInKB();
    }

    public void setBatchlogReplayThrottleInKB(int throttleInKB) {
        DatabaseDescriptor.setBatchlogReplayThrottleInKB(throttleInKB);
        BatchlogManager.instance.setRate(throttleInKB);
    }

    public int getConcurrentCompactors() {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    public void setConcurrentCompactors(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Number of concurrent compactors should be greater than 0.");
        } else {
            DatabaseDescriptor.setConcurrentCompactors(value);
            CompactionManager.instance.setConcurrentCompactors(value);
        }
    }

    public int getConcurrentValidators() {
        return DatabaseDescriptor.getConcurrentValidations();
    }

    public void setConcurrentValidators(int value) {
        DatabaseDescriptor.setConcurrentValidations(value);
        CompactionManager.instance.setConcurrentValidations(DatabaseDescriptor.getConcurrentValidations());
    }

    public int getConcurrentViewBuilders() {
        return DatabaseDescriptor.getConcurrentViewBuilders();
    }

    public void setConcurrentViewBuilders(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Number of concurrent view builders should be greater than 0.");
        } else {
            DatabaseDescriptor.setConcurrentViewBuilders(value);
            CompactionManager.instance.setConcurrentViewBuilders(DatabaseDescriptor.getConcurrentViewBuilders());
        }
    }

    public boolean isIncrementalBackupsEnabled() {
        return DatabaseDescriptor.isIncrementalBackupsEnabled();
    }

    public void setIncrementalBackupsEnabled(boolean value) {
        DatabaseDescriptor.setIncrementalBackupsEnabled(value);
    }

    private void setMode(StorageService.Mode m, boolean log) {
        this.setMode(m, (String) null, log);
    }

    private void setMode(StorageService.Mode m, String msg, boolean log) {
        this.operationMode = m;
        String logMsg = msg == null ? m.toString() : String.format("%s: %s", new Object[]{m, msg});
        if (log) {
            logger.info(logMsg);
        } else {
            logger.debug(logMsg);
        }

    }

    private boolean bootstrap(Collection<Token> tokens) {
        this.isBootstrapMode = true;
        this.updateTokensBlocking(tokens);
        if (this.replacing && isReplacingSameAddress()) {
            this.tokenMetadata.updateNormalTokens(tokens, FBUtilities.getBroadcastAddress());
            this.removeEndpointBlocking(DatabaseDescriptor.getReplaceAddress(), false);
        } else {
            List<Pair<ApplicationState, VersionedValue>> states = new ArrayList();
            states.add(Pair.create(ApplicationState.TOKENS, this.valueFactory.tokens(tokens)));
            states.add(Pair.create(ApplicationState.STATUS, this.replacing ? this.valueFactory.bootReplacing(DatabaseDescriptor.getReplaceAddress()) : this.valueFactory.bootstrapping(tokens)));
            Gossiper.instance.addLocalApplicationStates(states);
            this.setMode(StorageService.Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
            Uninterruptibles.sleepUninterruptibly((long) RING_DELAY, TimeUnit.MILLISECONDS);
        }

        if (!Gossiper.instance.seenAnySeed()) {
            throw new IllegalStateException("Unable to contact any seeds!");
        } else {
            if (PropertyConfiguration.getBoolean("cassandra.reset_bootstrap_progress")) {
                logger.info("Resetting bootstrap progress to start fresh");
                SystemKeyspace.resetAvailableRangesBlocking();
                SystemKeyspace.resetTransferredRanges();
            }

            this.invalidateDiskBoundaries();
            this.setMode(StorageService.Mode.JOINING, "Starting to bootstrap...", true);
            BootStrapper bootstrapper = new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, this.tokenMetadata);
            bootstrapper.addProgressListener(this.progressSupport);
            ListenableFuture bootstrapStream = bootstrapper.bootstrap(this.streamStateStore, !this.replacing && useStrictConsistency);

            try {
                bootstrapStream.get();
                this.bootstrapFinished();
                logger.info("Bootstrap completed for tokens {}", tokens);
                return true;
            } catch (Throwable var5) {
                logger.error("Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.", var5);
                return false;
            }
        }
    }

    private void invalidateDiskBoundaries() {
        for (Keyspace keyspace : Keyspace.all()) {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores()) {
                for (ColumnFamilyStore store : cfs.concatWithIndexes()) {
                    store.invalidateDiskBoundaries();
                }
            }
        }
    }

    private void markViewsAsBuiltBlocking() {
        ArrayList<CompletableFuture> futures = new ArrayList();
        for (String keyspace : Schema.instance.getUserKeyspaces()) {
            for (ViewMetadata view : Schema.instance.getKeyspaceMetadata(keyspace).views) {
                futures.add(SystemKeyspace.finishViewBuildStatus(view.keyspace, view.name));
            }
        }

        TPCUtils.blockingAwait(CompletableFuture.allOf((CompletableFuture[]) futures.toArray(new CompletableFuture[0])));
    }

    private void bootstrapFinished() {
        this.markViewsAsBuiltBlocking();
        this.isBootstrapMode = false;
    }

    public boolean resumeBootstrap() {
        if (this.isBootstrapMode && SystemKeyspace.bootstrapInProgress()) {
            logger.info("Resuming bootstrap...");
            Collection<Token> tokens = this.getSavedTokensBlocking();
            BootStrapper bootstrapper = new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, this.tokenMetadata);
            bootstrapper.addProgressListener(this.progressSupport);
            ListenableFuture<StreamState> bootstrapStream = bootstrapper.bootstrap(this.streamStateStore, useStrictConsistency && !this.replacing);
            Futures.addCallback(bootstrapStream, new FutureCallback<StreamState>() {
                public void onSuccess(StreamState streamState) {
                    StorageService.this.bootstrapFinished();
                    StorageService.this.isSurveyMode = true;

                    try {
                        StorageService.this.progressSupport.progress("bootstrap", ProgressEvent.createNotification("Joining ring..."));
                        StorageService.this.joinRing(true);
                    } catch (IOException var3) {
                        ;
                    }

                    StorageService.this.progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
                    StorageService.logger.info("Resume complete");
                }

                public void onFailure(Throwable e) {
                    String message = "Error during bootstrap: ";
                    if (e instanceof ExecutionException && e.getCause() != null) {
                        message = message + e.getCause().getMessage();
                    } else {
                        message = message + e.getMessage();
                    }

                    StorageService.logger.error(message, e);
                    StorageService.this.progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.ERROR, 1, 1, message));
                    StorageService.this.progressSupport.progress("bootstrap", new ProgressEvent(ProgressEventType.COMPLETE, 1, 1, "Resume bootstrap complete"));
                }
            });

            StreamState result;
            try {
                result = (StreamState) bootstrapStream.get();
            } catch (Exception var6) {
                throw Throwables.cleaned(var6);
            }

            if (!result.hasAbortedSession() && !result.hasFailedSession()) {
                return true;
            } else {
                throw Throwables.cleaned(new Exception("Failed to resume bootstrap, check logs"));
            }
        } else {
            logger.info("Resuming bootstrap is requested, but the node is already bootstrapped.");
            return false;
        }
    }

    public boolean isBootstrapMode() {
        return this.isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata() {
        return this.tokenMetadata;
    }

    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) {
        Map<List<String>, List<String>> map = new HashMap();
        for (Entry<Range<Token>, List<InetAddress>> entry : this.getRangeToAddressMap(keyspace).entrySet()) {
            map.put(((Range) entry.getKey()).asList(), this.stringify((Iterable) entry.getValue()));
        }

        return map;
    }

    /**
     * @deprecated
     */
    @Deprecated
    public String getRpcaddress(InetAddress endpoint) {
        return this.getNativeTransportAddress(endpoint);
    }

    public String getNativeTransportAddress(InetAddress endpoint) {
        return endpoint.equals(FBUtilities.getBroadcastAddress()) ? FBUtilities.getNativeTransportBroadcastAddress().getHostAddress() : (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_TRANSPORT_ADDRESS) == null ? endpoint.getHostAddress() : Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.NATIVE_TRANSPORT_ADDRESS).value);
    }

    /**
     * @deprecated
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace) {
        return this.getRangeToNativeTransportAddressMap(keyspace);
    }

    public Map<List<String>, List<String>> getRangeToNativeTransportAddressMap(String keyspace) {
        Map<List<String>, List<String>> map = new HashMap();

        for (Entry<Range<Token>, List<InetAddress>> entry : this.getRangeToAddressMap(keyspace).entrySet()) {
            List<String> nativeTransportAddresses = new ArrayList((entry.getValue()).size());
            for (InetAddress endpoint : entry.getValue()) {
                nativeTransportAddresses.add(this.getRpcaddress(endpoint));
            }
            map.put(((Range) entry.getKey()).asList(), nativeTransportAddresses);
        }

        return map;
    }

    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace) {
        if (keyspace == null) {
            keyspace = (String) Schema.instance.getNonLocalStrategyKeyspaces().get(0);
        }

        Map<List<String>, List<String>> map = new HashMap();

        this.tokenMetadata.getPendingRangesMM(keyspace).asMap().entrySet().forEach(rangeCollectionEntry -> {
            map.put(rangeCollectionEntry.getKey().asList(), this.stringify(new ArrayList(rangeCollectionEntry.getValue())));
        });

        return map;
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace) {
        return this.getRangeToAddressMap(keyspace, this.tokenMetadata.sortedTokens());
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMapInLocalDC(String keyspace) {
        com.google.common.base.Predicate<InetAddress> isLocalDC = new com.google.common.base.Predicate<InetAddress>() {
            public boolean apply(InetAddress address) {
                return StorageService.this.isLocalDC(address);
            }
        };
        Map<Range<Token>, List<InetAddress>> origMap = this.getRangeToAddressMap(keyspace, this.getTokensInLocalDC());
        Map<Range<Token>, List<InetAddress>> filteredMap = Maps.newHashMap();

        origMap.entrySet().forEach(rangeListEntry -> {
            filteredMap.put(rangeListEntry.getKey(), Lists.newArrayList(Collections2.filter((Collection) rangeListEntry.getValue(), isLocalDC)));
        });

        return filteredMap;
    }

    private List<Token> getTokensInLocalDC() {
        List<Token> filteredTokens = Lists.newArrayList();
        this.tokenMetadata.sortedTokens().forEach(token -> {
            if (this.isLocalDC(this.tokenMetadata.getEndpoint(token))) {
                filteredTokens.add(token);
            }
        });

        return filteredTokens;
    }

    private boolean isLocalDC(InetAddress targetHost) {
        return DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(targetHost);
    }

    private Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace, List<Token> sortedTokens) {
        if (keyspace == null) {
            keyspace = (String) Schema.instance.getNonLocalStrategyKeyspaces().get(0);
        }

        List<Range<Token>> ranges = this.getAllRanges(sortedTokens);
        return this.constructRangeToEndpointMap(keyspace, ranges);
    }

    public List<String> describeRingJMX(String keyspace) throws IOException {
        List<TokenRange> tokenRanges;
        try {
            tokenRanges = this.describeRing(keyspace);
        } catch (InvalidRequestException var6) {
            throw new IOException(var6.getMessage());
        }

        List<String> result = new ArrayList(tokenRanges.size());
        tokenRanges.forEach(tokenRange -> {
            result.add(tokenRange.toString());
        });

        return result;
    }

    public List<TokenRange> describeRing(String keyspace) throws InvalidRequestException {
        return this.describeRing(keyspace, false);
    }

    public List<TokenRange> describeLocalRing(String keyspace) throws InvalidRequestException {
        return this.describeRing(keyspace, true);
    }

    private List<TokenRange> describeRing(String keyspace, boolean includeOnlyLocalDC) throws InvalidRequestException {
        if (!Schema.instance.getKeyspaces().contains(keyspace)) {
            throw new InvalidRequestException("No such keyspace: " + keyspace);
        } else if (keyspace != null && !(Keyspace.open(keyspace).getReplicationStrategy() instanceof LocalStrategy)) {
            List<TokenRange> ranges = new ArrayList();
            Token.TokenFactory tf = this.getTokenFactory();
            Map<Range<Token>, List<InetAddress>> rangeToAddressMap = includeOnlyLocalDC ? this.getRangeToAddressMapInLocalDC(keyspace) : this.getRangeToAddressMap(keyspace);
            for (Entry<Range<Token>, List<InetAddress>> entry : rangeToAddressMap.entrySet()) {
                ranges.add(TokenRange.create(tf, (Range) entry.getKey(), (List) entry.getValue()));
            }

            return ranges;
        } else {
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);
        }
    }

    public Map<String, String> getTokenToEndpointMap() {
        Map<Token, InetAddress> mapInetAddress = this.tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap();
        Map<String, String> mapString = new LinkedHashMap(mapInetAddress.size());
        List<Token> tokens = new ArrayList(mapInetAddress.keySet());
        Collections.sort(tokens);

        for (Token token : tokens) {
            mapString.put(token.toString(), ((InetAddress) mapInetAddress.get(token)).getHostAddress());
        }

        return mapString;
    }

    public String getLocalHostId() {
        return this.getTokenMetadata().getHostId(FBUtilities.getBroadcastAddress()).toString();
    }

    public UUID getLocalHostUUID() {
        return this.getTokenMetadata().getHostId(FBUtilities.getBroadcastAddress());
    }

    public Map<String, String> getHostIdMap() {
        return this.getEndpointToHostId();
    }

    public Map<String, String> getEndpointToHostId() {
        Map<String, String> mapOut = new HashMap();
        for (Entry<InetAddress, UUID> entry : this.getTokenMetadata().getEndpointToHostIdMapForReading().entrySet()) {
            mapOut.put(((InetAddress) entry.getKey()).getHostAddress(), ((UUID) entry.getValue()).toString());
        }

        return mapOut;
    }

    public Map<String, String> getHostIdToEndpoint() {
        Map<String, String> mapOut = new HashMap();
        for (Entry<InetAddress, UUID> entry : this.getTokenMetadata().getEndpointToHostIdMapForReading().entrySet()) {
            mapOut.put(((UUID) entry.getValue()).toString(), ((InetAddress) entry.getKey()).getHostAddress());
        }

        return mapOut;
    }

    private Map<Range<Token>, List<InetAddress>> constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges) {
        Map<Range<Token>, List<InetAddress>> rangeToEndpointMap = new HashMap(ranges.size());
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        for (Range<Token> range : ranges) {
            rangeToEndpointMap.put(range, strategy.getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
    }

    public void onStarted(InetAddress endpoint, boolean isNew, EndpointState state) {
        VersionedValue schemaCompatVersion = state.getApplicationState(ApplicationState.SCHEMA_COMPATIBILITY_VERSION);
        if (schemaCompatVersion != null) {
            Schema.instance.updateEndpointCompatibilityVersion(endpoint, Integer.valueOf(schemaCompatVersion.value).intValue());
        }

    }

    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
    }

    public void onChange(final InetAddress endpoint, final ApplicationState state, final VersionedValue value) {
        if (state == ApplicationState.STATUS) {
            final String[] pieces = splitValue(value);
            assert pieces.length > 0;
            final String s;
            final String moveName = s = pieces[0];
            switch (s) {
                case "BOOT_REPLACE": {
                    this.handleStateBootreplacing(endpoint, pieces);
                    break;
                }
                case "BOOT": {
                    this.handleStateBootstrap(endpoint);
                    break;
                }
                case "NORMAL": {
                    this.handleStateNormal(endpoint, "NORMAL");
                    break;
                }
                case "shutdown": {
                    this.handleStateNormal(endpoint, "shutdown");
                    break;
                }
                case "removing":
                case "removed": {
                    this.handleStateRemoving(endpoint, pieces);
                    break;
                }
                case "LEAVING": {
                    this.handleStateLeaving(endpoint);
                    break;
                }
                case "LEFT": {
                    this.handleStateLeft(endpoint, pieces);
                    break;
                }
                case "MOVING": {
                    this.handleStateMoving(endpoint, pieces);
                    break;
                }
            }
        } else {
            final EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState)) {
                StorageService.logger.debug("Ignoring state change for dead or unknown endpoint: {}", (Object) endpoint);
                return;
            }
            if (this.getTokenMetadata().isMember(endpoint)) {
                switch (state) {
                    case RELEASE_VERSION: {
                        this.updatePeerInfoBlocking(endpoint, "release_version", value.value);
                        break;
                    }
                    case DC: {
                        this.updateTopology(endpoint);
                        this.updatePeerInfoBlocking(endpoint, "data_center", value.value);
                        break;
                    }
                    case RACK: {
                        this.updateTopology(endpoint);
                        this.updatePeerInfoBlocking(endpoint, "rack", value.value);
                        break;
                    }
                    case NATIVE_TRANSPORT_ADDRESS: {
                        try {
                            final InetAddress address = InetAddress.getByName(value.value);
                            this.updatePeerInfoBlocking(endpoint, "rpc_address", address);
                            this.updatePeerInfoBlocking(endpoint, "native_transport_address", address);
                            break;
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    case SCHEMA: {
                        this.updatePeerInfoBlocking(endpoint, "schema_version", UUID.fromString(value.value));
                        final MigrationManager instance = MigrationManager.instance;
                        MigrationManager.scheduleSchemaPull(endpoint, epState, String.format("gossip schema version change to %s", value.value));
                        break;
                    }
                    case HOST_ID: {
                        this.updatePeerInfoBlocking(endpoint, "host_id", UUID.fromString(value.value));
                        break;
                    }
                    case NATIVE_TRANSPORT_READY: {
                        this.notifyNativeTransportChange(endpoint, epState.isRpcReady());
                        break;
                    }
                    case NET_VERSION: {
                        this.updateNetVersion(endpoint, value);
                        break;
                    }
                    case NATIVE_TRANSPORT_PORT: {
                        SystemKeyspace.updatePeerInfo(endpoint, "native_transport_port", Integer.parseInt(value.value));
                        break;
                    }
                    case NATIVE_TRANSPORT_PORT_SSL: {
                        SystemKeyspace.updatePeerInfo(endpoint, "native_transport_port_ssl", Integer.parseInt(value.value));
                        break;
                    }
                    case STORAGE_PORT: {
                        SystemKeyspace.updatePeerInfo(endpoint, "storage_port", Integer.parseInt(value.value));
                        break;
                    }
                    case STORAGE_PORT_SSL: {
                        SystemKeyspace.updatePeerInfo(endpoint, "storage_port_ssl", Integer.parseInt(value.value));
                        break;
                    }
                    case JMX_PORT: {
                        SystemKeyspace.updatePeerInfo(endpoint, "jmx_port", Integer.parseInt(value.value));
                        break;
                    }
                    case DSE_GOSSIP_STATE: {
                        final Map<String, Object> updates = new HashMap<String, Object>();
                        this.updateDseState(value, endpoint, updates);
                        this.updatePeerInfo(endpoint, updates);
                        break;
                    }
                }
            }
        }
    }

    private static String[] splitValue(VersionedValue value) {
        return value.value.split(VersionedValue.DELIMITER_STR, -1);
    }

    private void updateNetVersion(InetAddress endpoint, VersionedValue value) {
        try {
            org.apache.cassandra.net.ProtocolVersion v = org.apache.cassandra.net.ProtocolVersion.fromHandshakeVersion(Integer.parseInt(value.value));
            MessagingService.instance().setVersion(endpoint, MessagingVersion.from(v));
        } catch (NumberFormatException var4) {
            throw new AssertionError("Got invalid value for NET_VERSION application state: " + value.value);
        }
    }

    public void updateTopology(InetAddress endpoint) {
        if (this.getTokenMetadata().isMember(endpoint)) {
            this.getTokenMetadata().updateTopology(endpoint);
        }

    }

    public void updateTopology() {
        this.getTokenMetadata().updateTopology();
    }

    private void updatePeerInfoBlocking(InetAddress ep, String columnName, Object value) {
        TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(ep, columnName, value));
    }


    private void updatePeerInfoBlocking(final InetAddress endpoint) {
        final Map<String, Object> updates = new HashMap<String, Object>();
        final EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        for (final Map.Entry<ApplicationState, VersionedValue> entry : epState.states()) {
            switch (entry.getKey()) {
                case RELEASE_VERSION: {
                    updates.put("release_version", entry.getValue().value);
                    continue;
                }
                case DC: {
                    updates.put("data_center", entry.getValue().value);
                    continue;
                }
                case RACK: {
                    updates.put("rack", entry.getValue().value);
                    continue;
                }
                case NATIVE_TRANSPORT_ADDRESS: {
                    try {
                        final InetAddress address = InetAddress.getByName(entry.getValue().value);
                        updates.put("rpc_address", address);
                        updates.put("native_transport_address", address);
                        continue;
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
                case SCHEMA: {
                    updates.put("schema_version", UUID.fromString(entry.getValue().value));
                    continue;
                }
                case HOST_ID: {
                    updates.put("host_id", UUID.fromString(entry.getValue().value));
                    continue;
                }
                case NATIVE_TRANSPORT_PORT: {
                    updates.put("native_transport_port", Integer.parseInt(entry.getValue().value));
                    continue;
                }
                case NATIVE_TRANSPORT_PORT_SSL: {
                    updates.put("native_transport_port_ssl", Integer.parseInt(entry.getValue().value));
                    continue;
                }
                case STORAGE_PORT: {
                    updates.put("storage_port", Integer.parseInt(entry.getValue().value));
                    continue;
                }
                case STORAGE_PORT_SSL: {
                    updates.put("storage_port_ssl", Integer.parseInt(entry.getValue().value));
                    continue;
                }
                case JMX_PORT: {
                    updates.put("jmx_port", Integer.parseInt(entry.getValue().value));
                    continue;
                }
                case DSE_GOSSIP_STATE: {
                    this.updateDseState(entry.getValue(), endpoint, updates);
                    continue;
                }
            }
        }
        this.updatePeerInfo(endpoint, updates);
    }

    private void updateDseState(VersionedValue versionedValue, InetAddress endpoint, Map<String, Object> updates) {
        Map<String, Object> dseState = DseState.getValues(versionedValue);
        DseEndpointState dseEndpointState = (DseEndpointState) this.dseEndpointStates.computeIfAbsent(endpoint, (i) -> {
            return new DseEndpointState();
        });
        boolean active = DseState.getActiveStatus(dseState);
        dseEndpointState.setActive(active);
        dseEndpointState.setBlacklisted(DseState.getBlacklistingStatus(dseState));
        dseEndpointState.setNodeHealth(DseState.getNodeHealth(dseState));
        Map<String, DseState.CoreIndexingStatus> cores = DseState.getCoreIndexingStatus(dseState);
        if (cores != null) {
            dseEndpointState.setCoreIndexingStatus(cores);
        }

        PeerInfo peerInfo = SystemKeyspace.getPeerInfo(endpoint);
        Set<Workload> workloads = DseState.getWorkloads(dseState, endpoint);
        ProductVersion.Version version = DseState.getDseVersion(dseState);
        String serverId = DseState.getServerID(dseState);
        Boolean isGraph = DseState.getIsGraphNode(dseState);
        if (serverId != null) {
            this.checkRackAndServerId(endpoint, serverId);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Workloads of {}: {}. Endpoint is {}, version is {}", new Object[]{endpoint.getHostAddress(), workloads, active ? "active" : "not active", version});
        }

        if (workloads != null && (peerInfo == null || peerInfo.getWorkloads() == null || !peerInfo.getWorkloads().equals(workloads))) {
            this.logDseStateUpdate(endpoint, "workloads", active, workloads, workloads);
            updates.put("workload", Workload.legacyWorkloadName(workloads));
            updates.put("workloads", Workload.toStringSet(workloads));
        }

        if (version != null && (peerInfo == null || peerInfo.getDseVersion() == null || !peerInfo.getDseVersion().equals(version))) {
            this.logDseStateUpdate(endpoint, "DSE version", active, workloads, version);
            updates.put("dse_version", version.toString());
        }

        if (serverId != null && (peerInfo == null || peerInfo.getServerId() == null || !peerInfo.getServerId().equals(serverId))) {
            this.logDseStateUpdate(endpoint, "server ID", active, workloads, serverId);
            updates.put("server_id", serverId);
        }

        if (isGraph != null && (peerInfo == null || peerInfo.getGraph() == null || !peerInfo.getGraph().equals(isGraph))) {
            this.logDseStateUpdate(endpoint, "graph server value", active, workloads, isGraph);
            updates.put("graph", isGraph);
        }

    }

    private void logDseStateUpdate(InetAddress endpoint, String what, boolean active, Set<Workload> workloads, Object value) {
        if (logger.isDebugEnabled()) {
            logger.debug("Updating {} of {} ({}, {}) to {}", new Object[]{what, endpoint.getHostAddress(), active ? "active" : "not active", workloads, value});
        }

    }

    private void updatePeerInfo(InetAddress endpoint, Map<String, Object> updates) {
        if (!updates.isEmpty()) {
            if (!FBUtilities.isLocalEndpoint(endpoint)) {
                TPCUtils.blockingAwait(SystemKeyspace.updatePeerInfo(endpoint, updates));
            } else if (!this.isShutdown) {
                Completable complete = Completable.concat((Iterable) updates.entrySet().stream().map((entry) -> {
                    return Completable.fromFuture(SystemKeyspace.updateLocalInfo((String) entry.getKey(), entry.getValue()));
                }).collect(Collectors.toList()));
                TPCUtils.blockingAwait(complete);
            }

        }
    }

    private void checkRackAndServerId(InetAddress endpoint, String endpointServerId) {
        InetAddress self = FBUtilities.getBroadcastAddress();
        if (self.equals(endpoint)) {
            return;
        }
        String localId = ServerId.getServerId();
        if (localId != null && endpointServerId != null) {
            if (localId.equals(endpointServerId)) {
                IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
                InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
                String localDC = snitch.getDatacenter(broadcastAddress);
                String endpointDC = snitch.getDatacenter(endpoint);
                String localRack = snitch.getRack(broadcastAddress);
                String endpointRack = snitch.getRack(endpoint);
                if (localDC.equals(endpointDC) && !localRack.equals(endpointRack)) {
                    String msg = "%s has the same server ID %s, and same DC %s, but is placed in a different rack %s - (%s, %s, %s) vs. (%s, %s, %s)";
                    logger.warn(String.format(msg, new Object[]{endpoint, endpointServerId, endpointDC, endpointRack, broadcastAddress, localDC, localRack, endpoint, endpointDC, endpointRack}));
                }

            }
        }
    }

    public Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus(InetAddress endpoint) {
        return ((DseEndpointState) this.dseEndpointStates.getOrDefault(endpoint, DseEndpointState.nullDseEndpointState)).getCoreIndexingStatus();
    }

    public boolean getBlacklistedStatus(InetAddress endpoint) {
        return ((DseEndpointState) this.dseEndpointStates.getOrDefault(endpoint, DseEndpointState.nullDseEndpointState)).isBlacklisted();
    }

    public Double getNodeHealth(InetAddress endpoint) {
        return Double.valueOf(((DseEndpointState) this.dseEndpointStates.getOrDefault(endpoint, DseEndpointState.nullDseEndpointState)).getNodeHealth());
    }

    public boolean getActiveStatus(InetAddress endpoint) {
        return ((DseEndpointState) this.dseEndpointStates.getOrDefault(endpoint, DseEndpointState.nullDseEndpointState)).isActive();
    }

    private void notifyNativeTransportChange(InetAddress endpoint, boolean ready) {
        if (ready) {
            this.notifyUp(endpoint);
        } else {
            this.notifyDown(endpoint);
        }

    }

    private void notifyUp(InetAddress endpoint) {
        if (this.isRpcReady(endpoint) && Gossiper.instance.isAlive(endpoint)) {
            this.lifecycleSubscribers.forEach(iEndpointLifecycleSubscriber ->
            {
                iEndpointLifecycleSubscriber.onUp(endpoint);
            });
        }
    }

    private void notifyDown(InetAddress endpoint) {
        this.lifecycleSubscribers.forEach(iEndpointLifecycleSubscriber ->
        {
            iEndpointLifecycleSubscriber.onDown(endpoint);
        });
    }

    private void notifyJoined(InetAddress endpoint) {
        if (this.isStatus(endpoint, "NORMAL")) {
            this.lifecycleSubscribers.forEach(iEndpointLifecycleSubscriber ->
            {
                iEndpointLifecycleSubscriber.onJoinCluster(endpoint);
            });
        }
    }

    private void notifyMoved(InetAddress endpoint) {
        this.lifecycleSubscribers.forEach(iEndpointLifecycleSubscriber ->
        {
            iEndpointLifecycleSubscriber.onMove(endpoint);
        });
    }

    private void notifyLeft(InetAddress endpoint) {
        this.lifecycleSubscribers.forEach(iEndpointLifecycleSubscriber ->
        {
            iEndpointLifecycleSubscriber.onLeaveCluster(endpoint);
        });
    }

    private boolean isStatus(InetAddress endpoint, String status) {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        return state != null && state.getStatus().equals(status);
    }

    public boolean isRpcReady(InetAddress endpoint) {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        return state != null && state.isRpcReady();
    }

    public void setNativeTransportReady(boolean value) {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());

        assert !value || state != null;

        if (state != null) {
            Gossiper.instance.updateLocalApplicationState(ApplicationState.NATIVE_TRANSPORT_READY, this.valueFactory.nativeTransportReady(value));
        }

    }

    private Collection<Token> getTokensFor(InetAddress endpoint) {
        return Gossiper.instance.getTokensFor(endpoint, this.tokenMetadata.partitioner);
    }

    private void handleStateBootstrap(InetAddress endpoint) {
        Collection<Token> tokens = this.getTokensFor(endpoint);
        if (logger.isDebugEnabled()) {
            logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);
        }

        if (this.tokenMetadata.isMember(endpoint)) {
            if (!this.tokenMetadata.isLeaving(endpoint)) {
                logger.info("Node {} state jump to bootstrap", endpoint);
            }

            this.tokenMetadata.removeEndpoint(endpoint);
        }

        this.tokenMetadata.addBootstrapTokens(tokens, endpoint);
        PendingRangeCalculatorService.instance.update();
        this.tokenMetadata.updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
    }

    private void handleStateBootreplacing(InetAddress newNode, String[] pieces) {
        InetAddress oldNode;
        try {
            oldNode = InetAddress.getByName(pieces[1]);
        } catch (Exception var6) {
            logger.error("Node {} tried to replace malformed endpoint {}.", new Object[]{newNode, pieces[1], var6});
            return;
        }

        if (FailureDetector.instance.isAlive(oldNode)) {
            throw new RuntimeException(String.format("Node %s is trying to replace alive node %s.", new Object[]{newNode, oldNode}));
        } else {
            Optional<InetAddress> replacingNode = this.tokenMetadata.getReplacingNode(newNode);
            if (replacingNode.isPresent() && !((InetAddress) replacingNode.get()).equals(oldNode)) {
                throw new RuntimeException(String.format("Node %s is already replacing %s but is trying to replace %s.", new Object[]{newNode, replacingNode.get(), oldNode}));
            } else {
                Collection<Token> tokens = this.getTokensFor(newNode);
                if (logger.isDebugEnabled()) {
                    logger.debug("Node {} is replacing {}, tokens {}", new Object[]{newNode, oldNode, tokens});
                }

                this.tokenMetadata.addReplaceTokens(tokens, newNode, oldNode);
                PendingRangeCalculatorService.instance.update();
                this.tokenMetadata.updateHostId(Gossiper.instance.getHostId(newNode), newNode);
            }
        }
    }

    private void handleStateNormal(final InetAddress endpoint, final String status) {
        final Collection<Token> tokens = this.getTokensFor(endpoint);
        final Set<Token> tokensToUpdateInMetadata = SetsFactory.newSet();
        final Set<Token> tokensToUpdateInSystemKeyspace = SetsFactory.newSet();
        final Set<InetAddress> endpointsToRemove = SetsFactory.newSet();
        if (StorageService.logger.isDebugEnabled()) {
            StorageService.logger.debug("Node {} state {}, token {}", new Object[]{endpoint, status, tokens});
        }
        if (this.tokenMetadata.isMember(endpoint)) {
            StorageService.logger.info("Node {} state jump to {}", (Object) endpoint, (Object) status);
        }
        if (tokens.isEmpty() && status.equals("NORMAL")) {
            StorageService.logger.error("Node {} is in state normal but it has no tokens, state: {}", (Object) endpoint, (Object) Gossiper.instance.getEndpointStateForEndpoint(endpoint));
        }
        final Optional<InetAddress> replacingNode = this.tokenMetadata.getReplacingNode(endpoint);
        if (replacingNode.isPresent()) {
            assert !endpoint.equals(replacingNode.get()) : "Pending replacement endpoint with same address is not supported";
            StorageService.logger.info("Node {} will complete replacement of {} for tokens {}", new Object[]{endpoint, replacingNode.get(), tokens});
            if (FailureDetector.instance.isAlive(replacingNode.get())) {
                StorageService.logger.error("Node {} cannot complete replacement of alive node {}.", (Object) endpoint, (Object) replacingNode.get());
                return;
            }
            endpointsToRemove.add(replacingNode.get());
        }
        final Optional<InetAddress> replacementNode = this.tokenMetadata.getReplacementNode(endpoint);
        if (replacementNode.isPresent()) {
            StorageService.logger.warn("Node {} is currently being replaced by node {}.", (Object) endpoint, (Object) replacementNode.get());
        }
        this.updatePeerInfoBlocking(endpoint);
        final UUID hostId = Gossiper.instance.getHostId(endpoint);
        final InetAddress existing = this.tokenMetadata.getEndpointForHostId(hostId);
        if (this.replacing && isReplacingSameAddress() && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null && hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress()))) {
            StorageService.logger.warn("Not updating token metadata for {} because I am replacing it", (Object) endpoint);
        } else if (existing != null && !existing.equals(endpoint)) {
            if (existing.equals(FBUtilities.getBroadcastAddress())) {
                StorageService.logger.warn("Not updating host ID {} for {} because it's mine", (Object) hostId, (Object) endpoint);
                this.tokenMetadata.removeEndpoint(endpoint);
                endpointsToRemove.add(endpoint);
            } else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0) {
                StorageService.logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", new Object[]{hostId, existing, endpoint, endpoint});
                this.tokenMetadata.removeEndpoint(existing);
                endpointsToRemove.add(existing);
                this.tokenMetadata.updateHostId(hostId, endpoint);
            } else {
                StorageService.logger.warn("Host ID collision for {} between {} and {}; ignored {}", new Object[]{hostId, existing, endpoint, endpoint});
                this.tokenMetadata.removeEndpoint(endpoint);
                endpointsToRemove.add(endpoint);
            }
        } else {
            this.tokenMetadata.updateHostId(hostId, endpoint);
        }
        for (final Token token : tokens) {
            final InetAddress currentOwner = this.tokenMetadata.getEndpoint(token);
            if (currentOwner == null) {
                StorageService.logger.debug("New node {} at token {}", (Object) endpoint, (Object) token);
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            } else if (endpoint.equals(currentOwner)) {
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            } else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0) {
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
                final Multimap<InetAddress, Token> epToTokenCopy = this.getTokenMetadata().getEndpointToTokenMapForReading();
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).size() < 1) {
                    endpointsToRemove.add(currentOwner);
                }
                StorageService.logger.info("Nodes {} and {} have the same token {}.  {} is the new owner", new Object[]{endpoint, currentOwner, token, endpoint});
            } else {
                StorageService.logger.info("Nodes {} and {} have the same token {}.  Ignoring {}", new Object[]{endpoint, currentOwner, token, endpoint});
            }
        }
        final boolean isMember = this.tokenMetadata.isMember(endpoint);
        final boolean isMoving = this.tokenMetadata.isMoving(endpoint);
        this.tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (final InetAddress ep : endpointsToRemove) {
            this.removeEndpointBlocking(ep);
            if (this.replacing && DatabaseDescriptor.getReplaceAddress().equals(ep)) {
                Gossiper.instance.replacementQuarantine(ep);
            }
        }
        if (!tokensToUpdateInSystemKeyspace.isEmpty()) {
            this.updateTokensBlocking(endpoint, tokensToUpdateInSystemKeyspace);
        }
        if (isMoving || this.operationMode == Mode.MOVING) {
            this.tokenMetadata.removeFromMoving(endpoint);
            this.notifyMoved(endpoint);
        } else if (!isMember) {
            this.notifyJoined(endpoint);
        }
        PendingRangeCalculatorService.instance.update();
    }

    private void handleStateLeaving(InetAddress endpoint) {
        Collection<Token> tokens = this.getTokensFor(endpoint);
        if (logger.isDebugEnabled()) {
            logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);
        }

        if (!this.tokenMetadata.isMember(endpoint)) {
            logger.info("Node {} state jump to leaving", endpoint);
            this.tokenMetadata.updateNormalTokens(tokens, endpoint);
        } else if (!this.tokenMetadata.getTokens(endpoint).containsAll(tokens)) {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            this.tokenMetadata.updateNormalTokens(tokens, endpoint);
        }

        this.tokenMetadata.addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    private void handleStateLeft(InetAddress endpoint, String[] pieces) {
        assert pieces.length >= 2;

        Collection<Token> tokens = this.getTokensFor(endpoint);
        if (logger.isDebugEnabled()) {
            logger.debug("Node {} state left, tokens {}", endpoint, tokens);
        }

        this.excise(tokens, endpoint, this.extractExpireTime(pieces));
    }

    private void handleStateMoving(InetAddress endpoint, String[] pieces) {
        assert pieces.length >= 2;

        Token token = this.getTokenFactory().fromString(pieces[1]);
        if (logger.isDebugEnabled()) {
            logger.debug("Node {} state moving, new token {}", endpoint, token);
        }

        this.tokenMetadata.addMovingEndpoint(token, endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    private void handleStateRemoving(InetAddress endpoint, String[] pieces) {
        assert pieces.length > 0;

        if (endpoint.equals(FBUtilities.getBroadcastAddress())) {
            logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");

            try {
                this.drain();
            } catch (Exception var7) {
                throw new RuntimeException(var7);
            }
        } else {
            if (this.tokenMetadata.isMember(endpoint)) {
                String state = pieces[0];
                Collection<Token> removeTokens = this.tokenMetadata.getTokens(endpoint);
                if ("removed".equals(state)) {
                    this.excise(removeTokens, endpoint, this.extractExpireTime(pieces));
                } else if ("removing".equals(state)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);
                    }

                    this.tokenMetadata.addLeavingEndpoint(endpoint);
                    PendingRangeCalculatorService.instance.update();
                    String[] coordinator = splitValue(Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR));
                    UUID hostId = UUID.fromString(coordinator[1]);
                    this.restoreReplicaCount(endpoint, this.tokenMetadata.getEndpointForHostId(hostId));
                }
            } else {
                if ("removed".equals(pieces[0])) {
                    this.addExpireTimeIfFound(endpoint, this.extractExpireTime(pieces));
                }

                this.removeEndpointBlocking(endpoint);
            }

        }
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint) {
        logger.info("Removing tokens {} for {}", tokens, endpoint);
        UUID hostId = this.tokenMetadata.getHostId(endpoint);
        if (hostId != null && this.tokenMetadata.isMember(endpoint)) {
            long delay = DatabaseDescriptor.getMinRpcTimeout() + DatabaseDescriptor.getWriteRpcTimeout();
            ScheduledExecutors.optionalTasks.schedule(() -> {
                HintsService.instance.excise(hostId);
            }, delay, TimeUnit.MILLISECONDS);
        }

        this.removeEndpointBlocking(endpoint);
        this.tokenMetadata.removeEndpoint(endpoint);
        if (!tokens.isEmpty()) {
            this.tokenMetadata.removeBootstrapTokens(tokens);
        }

        this.notifyLeft(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint, long expireTime) {
        this.addExpireTimeIfFound(endpoint, expireTime);
        this.excise(tokens, endpoint);
    }

    private void removeEndpointBlocking(InetAddress endpoint) {
        this.removeEndpointBlocking(endpoint, true);
    }

    private void removeEndpointBlocking(InetAddress endpoint, boolean notifyGossip) {
        if (notifyGossip) {
            Gossiper.instance.removeEndpoint(endpoint);
        }

        TPCUtils.blockingAwait(SystemKeyspace.removeEndpoint(endpoint));
    }

    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime) {
        if (expireTime != 0L) {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }

    }

    protected long extractExpireTime(String[] pieces) {
        return Long.parseLong(pieces[2]);
    }

    private Multimap<InetAddress, Range<Token>> getNewSourceRanges(final String keyspaceName, final Set<Range<Token>> ranges) {
        final InetAddress myAddress = FBUtilities.getBroadcastAddress();
        final Multimap<Range<Token>, InetAddress> rangeAddresses = Keyspace.open(keyspaceName).getReplicationStrategy().getRangeAddresses(this.tokenMetadata.cloneOnlyTokenMap());
        final Multimap<InetAddress, Range<Token>> sourceRanges = HashMultimap.create();
        final IFailureDetector failureDetector = FailureDetector.instance;
        for (final Range<Token> range : ranges) {
            final Collection<InetAddress> possibleRanges = (Collection<InetAddress>) rangeAddresses.get(range);
            final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            final List<InetAddress> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);
            assert !sources.contains(myAddress);
            for (final InetAddress source : sources) {
                if (failureDetector.isAlive(source)) {
                    sourceRanges.put(source, range);
                    break;
                }
            }
        }
        return sourceRanges;
    }

    private void sendReplicationNotification(InetAddress remote) {
        Request request = Verbs.OPERATIONS.REPLICATION_FINISHED.newRequest(remote, EmptyPayload.instance);
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger.isDebugEnabled()) {
            logger.debug("Notifying {} of replication completion\n", (Object) remote);
        }
        while (failureDetector.isAlive(remote)) {
            CompletableFuture future = MessagingService.instance().sendSingleTarget(request);
            try {
                Uninterruptibles.getUninterruptibly(future);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof CallbackExpiredException) continue;
                logger.error("Unexpected exception when sending replication notification to " + remote, (Throwable) e);
                return;
            }
        }
    }

    private void restoreReplicaCount(final InetAddress endpoint, final InetAddress notifyEndpoint) {
        final Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();
        final InetAddress myAddress = FBUtilities.getBroadcastAddress();
        for (final String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces()) {
            final Multimap<Range<Token>, InetAddress> changedRanges = this.getChangedRangesForLeaving(keyspaceName, endpoint);
            final Set<Range<Token>> myNewRanges = SetsFactory.newSet();
            for (final Map.Entry<Range<Token>, InetAddress> entry : changedRanges.entries()) {
                if (entry.getValue().equals(myAddress)) {
                    myNewRanges.add(entry.getKey());
                }
            }
            final Multimap<InetAddress, Range<Token>> sourceRanges = this.getNewSourceRanges(keyspaceName, myNewRanges);
            for (final Map.Entry<InetAddress, Collection<Range<Token>>> entry2 : sourceRanges.asMap().entrySet()) {
                rangesToFetch.put(keyspaceName, entry2);
            }
        }
        final StreamPlan stream = new StreamPlan(StreamOperation.RESTORE_REPLICA_COUNT, true, true);
        for (final String keyspaceName2 : rangesToFetch.keySet()) {
            for (final Map.Entry<InetAddress, Collection<Range<Token>>> entry3 : rangesToFetch.get(keyspaceName2)) {
                final InetAddress source = entry3.getKey();
                final InetAddress preferred = SystemKeyspace.getPreferredIP(source);
                final Collection<Range<Token>> ranges = entry3.getValue();
                if (StorageService.logger.isDebugEnabled()) {
                    StorageService.logger.debug("Requesting from {} ranges {}", (Object) source, (Object) StringUtils.join((Iterable) ranges, ", "));
                }
                stream.requestRanges(source, preferred, keyspaceName2, ranges);
            }
        }
        final StreamResultFuture future = stream.execute();
        Futures.addCallback((ListenableFuture) future, (FutureCallback) new FutureCallback<StreamState>() {
            public void onSuccess(final StreamState finalState) {
                StorageService.this.sendReplicationNotification(notifyEndpoint);
            }

            public void onFailure(final Throwable t) {
                StorageService.logger.warn("Streaming to restore replica count failed", t);
                StorageService.this.sendReplicationNotification(notifyEndpoint);
            }
        });
    }


    private Multimap<Range<Token>, InetAddress> getChangedRangesForLeaving(String keyspaceName, InetAddress endpoint) {
        Collection<Range<Token>> ranges = this.getRangesForEndpoint(keyspaceName, endpoint);
        if (logger.isDebugEnabled()) {
            logger.debug("Node {} ranges [{}]", endpoint, StringUtils.join(ranges, ", "));
        }

        Map<Range<Token>, List<InetAddress>> currentReplicaEndpoints = new HashMap(ranges.size());
        TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();

        for (Range<Token> range : ranges) {
            currentReplicaEndpoints.put(range, Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints((Token) range.right, metadata));
        }

        TokenMetadata temp = this.tokenMetadata.cloneAfterAllLeft();
        if (temp.isMember(endpoint)) {
            temp.removeEndpoint(endpoint);
        }

        Multimap<Range<Token>, InetAddress> changedRanges = HashMultimap.create();

        Range range;
        List newReplicaEndpoints;
        for (Iterator var8 = ranges.iterator(); var8.hasNext(); changedRanges.putAll(range, newReplicaEndpoints)) {
            range = (Range) var8.next();
            newReplicaEndpoints = Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints((Token) range.right, temp);
            newReplicaEndpoints.removeAll((Collection) currentReplicaEndpoints.get(range));
            if (logger.isDebugEnabled()) {
                if (newReplicaEndpoints.isEmpty()) {
                    logger.debug("Range {} already in all replicas", range);
                } else {
                    logger.debug("Range {} will be responsibility of {}", range, StringUtils.join(newReplicaEndpoints, ", "));
                }
            }
        }

        return changedRanges;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {
        for (Entry<ApplicationState, VersionedValue> entry : epState.states()) {
            this.onChange(endpoint, (ApplicationState) entry.getKey(), (VersionedValue) entry.getValue());
        }

        MigrationManager.scheduleSchemaPull(endpoint, epState, "endpoint joined");
    }

    public void onAlive(InetAddress endpoint, EndpointState state) {
        MigrationManager.scheduleSchemaPull(endpoint, state, "endpoint alive");
        if (this.tokenMetadata.isMember(endpoint)) {
            this.notifyUp(endpoint);
        }

    }

    public void onRemove(InetAddress endpoint) {
        this.dseEndpointStates.remove(endpoint);
        this.tokenMetadata.removeEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    public void onDead(InetAddress endpoint, EndpointState state) {
        MessagingService.instance().convict(endpoint).join();
        this.notifyDown(endpoint);
    }

    public void onRestart(InetAddress endpoint, EndpointState state) {
        if (state.isAlive()) {
            this.onDead(endpoint, state);
        }

        VersionedValue netVersion = state.getApplicationState(ApplicationState.NET_VERSION);
        if (netVersion != null) {
            this.updateNetVersion(endpoint, netVersion);
        }

    }

    public String getLoadString() {
        return FileUtils.stringifyFileSize((double) StorageMetrics.load.getCount());
    }

    public Map<String, String> getLoadMap() {
        Map<String, String> map = new HashMap();
        for (Entry<InetAddress, Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet()) {
            map.put(((InetAddress) entry.getKey()).getHostAddress(), FileUtils.stringifyFileSize(((Double) entry.getValue()).doubleValue()));
        }

        map.put(FBUtilities.getBroadcastAddress().getHostAddress(), this.getLoadString());
        return map;
    }

    public final void deliverHints(String host) {
        throw new UnsupportedOperationException();
    }

    private Collection<Token> getSavedTokensBlocking() {
        return (Collection) TPCUtils.blockingGet(SystemKeyspace.getSavedTokens());
    }

    private Collection<Token> getLocalTokensBlocking() {
        Collection<Token> tokens = this.getSavedTokensBlocking();
        logger.debug("Got tokens {}", tokens);

        assert tokens != null && !tokens.isEmpty();

        return tokens;
    }

    @Nullable
    public InetAddress getEndpointForHostId(UUID hostId) {
        return this.tokenMetadata.getEndpointForHostId(hostId);
    }

    @Nullable
    public UUID getHostIdForEndpoint(InetAddress address) {
        return this.tokenMetadata.getHostId(address);
    }

    public List<String> getTokens() {
        return this.getTokens(FBUtilities.getBroadcastAddress());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException {
        return this.getTokens(InetAddress.getByName(endpoint));
    }

    private List<String> getTokens(InetAddress endpoint) {
        ArrayList<String> strTokens = new ArrayList<String>();
        for (Token tok : this.getTokenMetadata().getTokens(endpoint)) {
            strTokens.add(tok.toString());
        }
        return strTokens;
    }

    public String getDSEReleaseVersion() {
        return ProductVersion.getDSEVersionString();
    }

    public String getReleaseVersion() {
        return ProductVersion.getReleaseVersionString();
    }

    public String getSchemaVersion() {
        return Schema.instance.getVersion().toString();
    }

    public List<String> getLeavingNodes() {
        return this.stringify(this.tokenMetadata.getLeavingEndpoints());
    }

    public List<String> getMovingNodes() {
        ArrayList<String> endpoints = new ArrayList<String>();
        for (Pair<Token, InetAddress> node : this.tokenMetadata.getMovingEndpoints()) {
            endpoints.add(((InetAddress) node.right).getHostAddress());
        }
        return endpoints;
    }

    public List<String> getJoiningNodes() {
        return this.stringify(this.tokenMetadata.getBootstrapTokens().valueSet());
    }

    public List<String> getLiveNodes() {
        return this.stringify(Gossiper.instance.getLiveMembers());
    }

    public Set<InetAddress> getLiveRingMembers() {
        return this.getLiveRingMembers(false);
    }

    public Set<InetAddress> getLiveRingMembers(boolean excludeDeadStates) {
        Set<InetAddress> ret = SetsFactory.newSet();
        for (InetAddress ep : Gossiper.instance.getLiveMembers()) {
            EndpointState epState;
            if (excludeDeadStates && ((epState = Gossiper.instance.getEndpointStateForEndpoint(ep)) == null || Gossiper.instance.isDeadState(epState)) || !this.tokenMetadata.isMember(ep))
                continue;
            ret.add(ep);
        }
        return ret;
    }

    public List<String> getUnreachableNodes() {
        return this.stringify(Gossiper.instance.getUnreachableMembers());
    }

    public String[] getAllDataFileLocations() {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();

        for (int i = 0; i < locations.length; ++i) {
            locations[i] = FileUtils.getCanonicalPath(locations[i]);
        }

        return locations;
    }

    public String getCommitLogLocation() {
        return FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation());
    }

    public String getSavedCachesLocation() {
        return FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation());
    }

    private List<String> stringify(Iterable<InetAddress> endpoints) {
        List<String> stringEndpoints = new ArrayList();

        for (InetAddress ep : endpoints) {
            stringEndpoints.add(ep.getHostAddress());
        }

        return stringEndpoints;
    }

    public int getCurrentGenerationNumber() {
        return Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddress());
    }

    public int forceKeyspaceCleanup(String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
        return this.forceKeyspaceCleanup(0, keyspaceName, tables);
    }

    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
        if (SchemaConstants.isLocalSystemKeyspace(keyspaceName)) {
            throw new RuntimeException("Cleanup of the system keyspace is neither necessary nor wise");
        } else {
            CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
            for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(false, false, keyspaceName, tables)) {
                CompactionManager.AllSSTableOpStatus oneStatus = cfStore.forceCleanup(jobs);
                if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                    status = oneStatus;
                }
            }

            return status.statusCode;
        }
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
        return this.scrub(disableSnapshot, skipCorrupted, true, 0, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
        return this.scrub(disableSnapshot, skipCorrupted, checkData, 0, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
        return this.scrub(disableSnapshot, skipCorrupted, checkData, false, jobs, keyspaceName, tables);
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs, String keyspaceName, String... tables) throws IOException, ExecutionException, InterruptedException {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(true, false, keyspaceName, tables)) {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, checkData, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                status = oneStatus;
            }
        }

        return status.statusCode;
    }

    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(false, false, keyspaceName, tableNames)) {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.verify(extendedVerify);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                status = oneStatus;
            }
        }

        return status.statusCode;
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        return this.upgradeSSTables(keyspaceName, excludeCurrentVersion, 0, tableNames);
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(true, true, keyspaceName, tableNames)) {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.sstablesRewrite(excludeCurrentVersion, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                status = oneStatus;
            }
        }

        return status.statusCode;
    }

    public void forceKeyspaceCompaction(boolean splitOutput, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(true, false, keyspaceName, tableNames)) {
            cfStore.forceMajorCompaction(splitOutput);
        }
    }

    public int relocateSSTables(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
        return this.relocateSSTables(0, keyspaceName, columnFamilies);
    }

    public int relocateSSTables(int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfs : this.getValidColumnFamilies(false, false, keyspaceName, columnFamilies)) {
            CompactionManager.AllSSTableOpStatus oneStatus = cfs.relocateSSTables(jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                status = oneStatus;
            }
        }
        return status.statusCode;
    }

    public int garbageCollect(String tombstoneOptionString, int jobs, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException {
        CompactionParams.TombstoneOption tombstoneOption = CompactionParams.TombstoneOption.valueOf(tombstoneOptionString);
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfs : this.getValidColumnFamilies(false, false, keyspaceName, columnFamilies)) {
            CompactionManager.AllSSTableOpStatus oneStatus = cfs.garbageCollect(tombstoneOption, jobs);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                status = oneStatus;
            }
        }
        return status.statusCode;
    }

    public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException {
        boolean skipFlush = Boolean.parseBoolean((String) options.getOrDefault("skipFlush", "false"));
        if (entities != null && entities.length > 0 && entities[0].contains(".")) {
            this.takeMultipleTableSnapshot(tag, skipFlush, entities);
        } else {
            this.takeSnapshot(tag, skipFlush, entities);
        }
    }

    public void takeTableSnapshot(String keyspaceName, String tableName, String tag) throws IOException {
        this.takeMultipleTableSnapshot(tag, false, new String[]{keyspaceName + "." + tableName});
    }

    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        Collection<Range<Token>> tokenRanges = this.createRepairRangeFrom(startToken, endToken);
        for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(true, false, keyspaceName, tableNames)) {
            cfStore.forceCompactionForTokenRange(tokenRanges);
        }
    }

    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException {
        this.takeSnapshot(tag, false, keyspaceNames);
    }

    public void takeMultipleTableSnapshot(String tag, String... tableList) throws IOException {
        this.takeMultipleTableSnapshot(tag, false, tableList);
    }

    private void takeSnapshot(String tag, boolean skipFlush, String... keyspaceNames) throws IOException {
        if (this.operationMode == StorageService.Mode.JOINING) {
            throw new IOException("Cannot snapshot until bootstrap completes");
        } else if (tag != null && !tag.equals("")) {
            Iterable<Keyspace> keyspaces;
            if (keyspaceNames.length == 0) {
                keyspaces = Keyspace.all();
            } else {
                ArrayList<Keyspace> t = new ArrayList(keyspaceNames.length);
                String[] var6 = keyspaceNames;
                int var7 = keyspaceNames.length;

                for (int var8 = 0; var8 < var7; ++var8) {
                    String keyspaceName = var6[var8];
                    t.add(this.getValidKeyspace(keyspaceName));
                }

                keyspaces = t;
            }

            for (Keyspace keyspace : keyspaces) {
                if (keyspace.snapshotExists(tag)) {
                    throw new IOException("Snapshot " + tag + " already exists.");
                }
            }

            Set<SSTableReader> snapshotted = SetsFactory.newSet();

            for (Keyspace keyspace : keyspaces) {
                snapshotted.addAll(keyspace.snapshot(tag, (String) null, skipFlush, snapshotted));
            }

        } else {
            throw new IOException("You must supply a snapshot name.");
        }
    }

    private /* varargs */ void takeMultipleTableSnapshot(String tag, boolean skipFlush, String... tableList) throws IOException {
        HashMap<Keyspace, List<String>> keyspaceColumnfamily = new HashMap();
        for (String table : tableList) {
            String tableName;
            Keyspace keyspace;
            String[] splittedString = StringUtils.split((String) table, (char) '.');
            if (splittedString.length == 2) {
                String keyspaceName = splittedString[0];
                tableName = splittedString[1];
                if (keyspaceName == null) {
                    throw new IOException("You must supply a keyspace name");
                }
                if (this.operationMode.equals((Object) Mode.JOINING)) {
                    throw new IOException("Cannot snapshot until bootstrap completes");
                }
                if (tableName == null) {
                    throw new IOException("You must supply a table name");
                }
                if (tag == null || tag.equals("")) {
                    throw new IOException("You must supply a snapshot name.");
                }
                keyspace = this.getValidKeyspace(keyspaceName);
                ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
                if (columnFamilyStore.snapshotExists(tag)) {
                    throw new IOException("Snapshot " + tag + " already exists.");
                }
                if (!keyspaceColumnfamily.containsKey(keyspace)) {
                    keyspaceColumnfamily.put(keyspace, new ArrayList());
                }
            } else {
                throw new IllegalArgumentException("Cannot take a snapshot on secondary index or invalid column family name. You must supply a column family name in the form of keyspace.columnfamily");
            }
            ((List) keyspaceColumnfamily.get(keyspace)).add(tableName);
        }
        Set<SSTableReader> snapshotted = SetsFactory.newSet();
        for (Map.Entry<Keyspace, List<String>> entry : keyspaceColumnfamily.entrySet()) {
            for (String table : entry.getValue()) {
                snapshotted.addAll(((Keyspace) entry.getKey()).snapshot(tag, table, skipFlush, snapshotted));
            }
        }
    }

    private void verifyKeyspaceIsValid(String keyspaceName) {
        if (null != Schema.instance.getVirtualKeyspaceInstance(keyspaceName)) {
            throw new IllegalArgumentException("Cannot perform any operations against keyspace " + keyspaceName);
        } else if (!Schema.instance.getKeyspaces().contains(keyspaceName)) {
            throw new IllegalArgumentException("Keyspace " + keyspaceName + " does not exist");
        }
    }

    private Keyspace getValidKeyspace(String keyspaceName) throws IOException {
        this.verifyKeyspaceIsValid(keyspaceName);
        return Keyspace.open(keyspaceName);
    }

    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException {
        if (tag == null) {
            tag = "";
        }

        Set<String> keyspaces = SetsFactory.newSet();

        for (int i = 0; i < DatabaseDescriptor.getAllDataFileLocations().length; ++i) {
            String dataDir = DatabaseDescriptor.getAllDataFileLocations()[i];
            String[] var8 = (new File(dataDir)).list();
            int var9 = var8.length;

            for (int var10 = 0; var10 < var9; ++var10) {
                String keyspaceDir = var8[var10];
                if (keyspaceNames.length <= 0 || Arrays.asList(keyspaceNames).contains(keyspaceDir)) {
                    keyspaces.add(keyspaceDir);
                }
            }
        }


        for (String keyspace : keyspaces) {
            Keyspace.clearSnapshot(tag, keyspace);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Cleared out snapshot directories");
        }

    }

    public Map<String, TabularData> getSnapshotDetails() {
        HashMap<String, TabularData> snapshotMap = new HashMap<String, TabularData>();
        for (Keyspace keyspace : Keyspace.all()) {
            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores()) {
                for (Map.Entry<String, Pair<Long, Long>> snapshotDetail : cfStore.getSnapshotDetails().entrySet()) {
                    TabularDataSupport data = (TabularDataSupport) snapshotMap.get(snapshotDetail.getKey());
                    if (data == null) {
                        data = new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE);
                        snapshotMap.put(snapshotDetail.getKey(), data);
                    }
                    SnapshotDetailsTabularData.from(snapshotDetail.getKey(), keyspace.getName(), cfStore.getTableName(), snapshotDetail, data);
                }
            }
        }
        return snapshotMap;
    }

    public long trueSnapshotsSize() {
        long total = 0L;
        for (Keyspace keyspace : Keyspace.all()) {
            if (SchemaConstants.isLocalSystemKeyspace(keyspace.getName())) continue;
            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores()) {
                total += cfStore.trueSnapshotsSize();
            }
        }
        return total;
    }

    public void refreshSizeEstimates() throws ExecutionException {
        this.cleanupSizeEstimates();
        FBUtilities.waitOnFuture(ScheduledExecutors.optionalTasks.submit(SizeEstimatesRecorder.instance));
    }

    public void cleanupSizeEstimates() {
        SetMultimap<String, String> sizeEstimates = (SetMultimap) TPCUtils.blockingGet(SystemKeyspace.getTablesWithSizeEstimates());

        for (Entry<String, Collection<String>> tablesByKeyspace : sizeEstimates.asMap().entrySet()) {
            String keyspace = tablesByKeyspace.getKey();
            if (!Schema.instance.getKeyspaces().contains(keyspace)) {
                TPCUtils.blockingGet(SystemKeyspace.clearSizeEstimates(keyspace));
            } else {
                for (String table : tablesByKeyspace.getValue()) {
                    if (Schema.instance.getTableMetadataRef(keyspace, table) == null) {
                        TPCUtils.blockingGet(SystemKeyspace.clearSizeEstimates(keyspace, table));
                    }
                }
            }
        }

        return;
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String keyspaceName, String... cfNames) throws IOException {
        Keyspace keyspace = this.getValidKeyspace(keyspaceName);
        return keyspace.getValidColumnFamilies(allowIndexes, autoAddIndexes, cfNames);
    }

    public void forceKeyspaceFlush(String keyspaceName, String... tableNames) throws IOException {
        for (ColumnFamilyStore cfStore : this.getValidColumnFamilies(true, false, keyspaceName, tableNames)) {
            logger.debug("Forcing flush on keyspace {}, CF {}", keyspaceName, cfStore.name);
            cfStore.forceBlockingFlush();
        }

    }

    public int repairAsync(String keyspace, Map<String, String> repairSpec) {
        RepairOption option = RepairOption.parse(repairSpec, this.tokenMetadata.partitioner);
        if (option.getRanges().isEmpty()) {
            if (option.isPrimaryRange()) {
                if (option.getDataCenters().isEmpty() && option.getHosts().isEmpty()) {
                    option.getRanges().addAll(this.getPrimaryRanges(keyspace));
                } else {
                    if (!option.isInLocalDCOnly()) {
                        throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
                    }

                    option.getRanges().addAll(this.getPrimaryRangesWithinDC(keyspace));
                }
            } else {
                option.getRanges().addAll(this.getLocalRanges(keyspace));
            }
        }

        if (!option.getRanges().isEmpty() && Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor() >= 2 && this.tokenMetadata.getAllEndpoints().size() >= 2) {
            if (option.isIncremental()) {
                this.failIfCannotRunIncrementalRepair(keyspace, (String[]) option.getColumnFamilies().toArray(new String[0]));
            }

            int cmd = nextRepairCommand.incrementAndGet();
            ActiveRepairService.repairCommandExecutor.execute(this.createRepairTask(cmd, keyspace, option));
            return cmd;
        } else {
            return 0;
        }
    }

    protected void failIfCannotRunIncrementalRepair(String keyspace, String[] tables) {
        try {
            Set<ColumnFamilyStore> tablesToRepair = Sets.newHashSet(this.getValidColumnFamilies(false, false, keyspace, tables));
            Set<String> tablesWithViewsOrCdc = (Set) tablesToRepair.stream().filter((c) -> {
                return c.hasViews() || c.metadata.get().isView() || c.isCdcEnabled();
            }).map((c) -> {
                return c.name;
            }).collect(Collectors.toSet());
            if (!tablesWithViewsOrCdc.isEmpty()) {
                throw new IllegalArgumentException(String.format("Cannot run incremental repair on tables %s from keyspace %s because incremental repair is not supported on tables with materialized views or CDC-enabled. Please run full repair on these tables.", new Object[]{tablesWithViewsOrCdc.toString(), keyspace}));
            }
        } catch (IOException var5) {
            throw new RuntimeException("Could not fetch tables for repair.", var5);
        }
    }

    @VisibleForTesting
    Collection<Range<Token>> createRepairRangeFrom(String beginToken, String endToken) {
        Token parsedBeginToken = this.getTokenFactory().fromString(beginToken);
        Token parsedEndToken = this.getTokenFactory().fromString(endToken);
        ArrayList<Range<Token>> repairingRange = new ArrayList();
        ArrayList<Token> tokens = new ArrayList(this.tokenMetadata.sortedTokens());
        if (!tokens.contains(parsedBeginToken)) {
            tokens.add(parsedBeginToken);
        }

        if (!tokens.contains(parsedEndToken)) {
            tokens.add(parsedEndToken);
        }

        Collections.sort(tokens);
        int start = tokens.indexOf(parsedBeginToken);
        int end = tokens.indexOf(parsedEndToken);

        for (int i = start; i != end; i = (i + 1) % tokens.size()) {
            Range<Token> range = new Range((RingPosition) tokens.get(i), (RingPosition) tokens.get((i + 1) % tokens.size()));
            repairingRange.add(range);
        }

        return repairingRange;
    }

    public Token.TokenFactory getTokenFactory() {
        return this.tokenMetadata.partitioner.getTokenFactory();
    }

    private FutureTask<Object> createRepairTask(int cmd, String keyspace, RepairOption options) {
        if (!options.getDataCenters().isEmpty() && !options.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter())) {
            throw new IllegalArgumentException("the local data center must be part of the repair");
        } else {
            RepairRunnable task = new RepairRunnable(this, cmd, options, keyspace);
            task.addProgressListener(this.progressSupport);
            if (options.isTraced()) {
                Runnable r = () -> {
                    try {
                        task.run();
                    } finally {
                        ExecutorLocals.set((ExecutorLocals) null);
                    }

                };
                return new FutureTask(r, null);
            } else {
                return new FutureTask(task, null);
            }
        }
    }

    public void forceTerminateAllRepairSessions() {
        ActiveRepairService.instance.terminateSessions();
    }

    @Nullable
    public List<String> getParentRepairStatus(int cmd) {
        Pair<ActiveRepairService.ParentRepairStatus, List<String>> pair = ActiveRepairService.instance.getRepairStatus(cmd);
        return pair == null ? null : UnmodifiableArrayList.<String>builder().add(((ActiveRepairService.ParentRepairStatus) ((Object) pair.left)).name()).addAll((Iterable) pair.right).build();
    }

    public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddress ep) {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = SetsFactory.newSet();
        TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();

        for (Token token : metadata.sortedTokens()) {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            if (endpoints.size() > 0 && ((InetAddress) endpoints.get(0)).equals(ep)) {
                primaryRanges.add(new Range(metadata.getPredecessor(token), token));
            }
        }

        return primaryRanges;
    }

    public Collection<Range<Token>> getPrimaryRangeForEndpointWithinDC(String keyspace, InetAddress referenceEndpoint) {
        TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(referenceEndpoint);
        Collection<InetAddress> localDcNodes = metadata.getTopology().getDatacenterEndpoints().get(localDC);
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> localDCPrimaryRanges = SetsFactory.newSet();

        for (Token token : metadata.sortedTokens()) {
            for (InetAddress endpoint : strategy.calculateNaturalEndpoints(token, metadata)) {
                if (localDcNodes.contains(endpoint)) {
                    if (endpoint.equals(referenceEndpoint)) {
                        localDCPrimaryRanges.add(new Range(metadata.getPredecessor(token), token));
                    }
                    break;
                }
            }
        }

        return localDCPrimaryRanges;
    }

    Collection<Range<Token>> getRangesForEndpoint(String keyspaceName, InetAddress ep) {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
    }

    public List<Range<Token>> getAllRanges(List<Token> sortedTokens) {
        if (logger.isTraceEnabled()) {
            logger.trace("computing ranges for {}", StringUtils.join(sortedTokens, ", "));
        }

        if (sortedTokens.isEmpty()) {
            return UnmodifiableArrayList.emptyList();
        } else {
            int size = sortedTokens.size();
            List<Range<Token>> ranges = new ArrayList(size + 1);

            for (int i = 1; i < size; ++i) {
                Range<Token> range = new Range((RingPosition) sortedTokens.get(i - 1), (RingPosition) sortedTokens.get(i));
                ranges.add(range);
            }

            Range<Token> range = new Range((RingPosition) sortedTokens.get(size - 1), (RingPosition) sortedTokens.get(0));
            ranges.add(range);
            return ranges;
        }
    }

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key) {
        KeyspaceMetadata ksMetaData = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (ksMetaData == null) {
            throw new IllegalArgumentException("Unknown keyspace '" + keyspaceName + "'");
        } else {
            TableMetadata metadata = ksMetaData.getTableOrViewNullable(cf);
            if (metadata == null) {
                throw new IllegalArgumentException("Unknown table '" + cf + "' in keyspace '" + keyspaceName + "'");
            } else {
                return this.getNaturalEndpoints((String) keyspaceName, (RingPosition) this.tokenMetadata.partitioner.getToken(metadata.partitionKeyType.fromString(key)));
            }
        }
    }

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key) {
        return this.getNaturalEndpoints((String) keyspaceName, (RingPosition) this.tokenMetadata.partitioner.getToken(key));
    }

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, RingPosition pos) {
        return this.getNaturalEndpoints(Keyspace.open(keyspaceName), pos);
    }

    public List<InetAddress> getNaturalEndpoints(Keyspace keyspace, RingPosition pos) {
        return keyspace.getReplicationStrategy().getNaturalEndpoints(pos);
    }

    public Iterable<InetAddress> getNaturalAndPendingEndpoints(String keyspaceName, Token token) {
        return Iterables.concat(this.getNaturalEndpoints((String) keyspaceName, (RingPosition) token), this.tokenMetadata.pendingEndpointsFor(token, keyspaceName));
    }

    public static void addLiveNaturalEndpointsToList(Keyspace keyspace, RingPosition pos, ArrayList<InetAddress> liveEps) {
        List<InetAddress> endpoints = keyspace.getReplicationStrategy().getCachedNaturalEndpoints(pos);
        int i = 0;

        for (int size = endpoints.size(); i < size; ++i) {
            InetAddress endpoint = (InetAddress) endpoints.get(i);
            if (FailureDetector.instance.isAlive(endpoint)) {
                liveEps.add(endpoint);
            }
        }

    }

    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception {
        ch.qos.logback.classic.Logger logBackLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(classQualifier);
        if (StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel)) {
            JMXConfiguratorMBean jmxConfiguratorMBean = (JMXConfiguratorMBean) JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"), JMXConfiguratorMBean.class);
            jmxConfiguratorMBean.reloadDefaultConfiguration();
        } else if (StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel)) {
            if (logBackLogger.getLevel() != null || this.hasAppenders(logBackLogger)) {
                logBackLogger.setLevel((Level) null);
            }

        } else {
            Level level = Level.toLevel(rawLevel);
            logBackLogger.setLevel(level);
            logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", new Object[]{level, classQualifier, rawLevel, rawLevel});
        }
    }

    public Map<String, String> getLoggingLevels() {
        LinkedHashMap logLevelMaps = Maps.newLinkedHashMap();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (ch.qos.logback.classic.Logger logger : lc.getLoggerList()) {
            if (logger.getLevel() == null && !this.hasAppenders(logger)) continue;
            logLevelMaps.put(logger.getName(), logger.getLevel().toString());
        }
        return logLevelMaps;
    }

    private boolean hasAppenders(ch.qos.logback.classic.Logger logger) {
        Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
        return it.hasNext();
    }

    public List<Pair<Range<Token>, Long>> getSplits(String keyspaceName, String cfName, Range<Token> range, int keysPerSplit) {
        Keyspace t = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        List<DecoratedKey> keys = this.keySamples(Collections.singleton(cfs), range);
        long totalRowCountEstimate = cfs.estimatedKeysForRange(range);
        int minSamplesPerSplit = 4;
        int maxSplitCount = keys.size() / minSamplesPerSplit + 1;
        int splitCount = Math.max(1, Math.min(maxSplitCount, (int) (totalRowCountEstimate / (long) keysPerSplit)));
        List<Token> tokens = this.keysToTokens(range, keys);
        return this.getSplits(tokens, splitCount, cfs);
    }

    private List<Pair<Range<Token>, Long>> getSplits(List<Token> tokens, int splitCount, ColumnFamilyStore cfs) {
        double step = (double) (tokens.size() - 1) / (double) splitCount;
        Token prevToken = tokens.get(0);
        List<Pair<Range<Token>, Long>> splits = Lists.newArrayListWithExpectedSize(splitCount);

        for (int i = 1; i <= splitCount; ++i) {
            int index = (int) Math.round((double) i * step);
            Token token = tokens.get(index);
            Range<Token> range = new Range(prevToken, token);
            splits.add(Pair.create(range, Long.valueOf(Math.max((long) cfs.metadata().params.minIndexInterval, cfs.estimatedKeysForRange(range)))));
            prevToken = token;
        }

        return splits;
    }

    private List<Token> keysToTokens(Range<Token> range, List<DecoratedKey> keys) {
        List<Token> tokens = Lists.newArrayListWithExpectedSize(keys.size() + 2);
        tokens.add(range.left);

        for (DecoratedKey key : keys) {
            tokens.add(key.getToken());
        }

        tokens.add(range.right);
        return tokens;
    }

    private List<DecoratedKey> keySamples(Iterable<ColumnFamilyStore> cfses, Range<Token> range) {
        List<DecoratedKey> keys = new ArrayList();

        for (ColumnFamilyStore cfs : cfses) {
            Iterables.addAll(keys, cfs.keySamples(range));
        }

        FBUtilities.sortSampledKeys(keys, range);
        return keys;
    }

    private void startLeaving() {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, this.valueFactory.leaving(this.getLocalTokensBlocking()));
        this.tokenMetadata.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
        PendingRangeCalculatorService.instance.update();
    }

    public void decommission(boolean force) throws InterruptedException {
        TokenMetadata metadata = this.tokenMetadata.cloneAfterAllLeft();
        if (this.operationMode != StorageService.Mode.LEAVING) {
            if (!this.tokenMetadata.isMember(FBUtilities.getBroadcastAddress())) {
                throw new UnsupportedOperationException("local node is not a member of the token ring yet");
            }

            if (metadata.getAllEndpoints().size() < 2) {
                throw new UnsupportedOperationException("no other normal nodes in the ring; decommission would be pointless");
            }

            if (this.operationMode != StorageService.Mode.NORMAL) {
                throw new UnsupportedOperationException("Node in " + this.operationMode + " state; wait for status to become normal or restart");
            }
        }

        if (!this.isDecommissioning.compareAndSet(false, true)) {
            throw new IllegalStateException("Node is still decommissioning. Check nodetool netstats.");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("DECOMMISSIONING");
        }

        try {
            CompletableFuture<Boolean> nodeSyncStopFuture = this.nodeSyncService.disableAsync(false);
            waitForNodeSyncShutdown(nodeSyncStopFuture);
            PendingRangeCalculatorService.instance.blockUntilFinished();
            String dc = DatabaseDescriptor.getLocalDataCenter();
            if (this.operationMode != StorageService.Mode.LEAVING) {
                for (String keyspaceName : Schema.instance.getPartitionedKeyspaces()) {
                    if (!force) {
                        Keyspace keyspace = Keyspace.open(keyspaceName);
                        int rf;
                        int numNodes;
                        if (keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy) {
                            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
                            rf = strategy.getReplicationFactor(dc);
                            numNodes = metadata.getTopology().getDatacenterEndpoints().get(dc).size();
                        } else {
                            numNodes = metadata.getAllEndpoints().size();
                            rf = keyspace.getReplicationStrategy().getReplicationFactor();
                        }

                        if (numNodes <= rf) {
                            throw new UnsupportedOperationException("Not enough live nodes to maintain replication factor in keyspace " + keyspaceName + " (RF = " + rf + ", N = " + numNodes + "). Perform a forceful decommission to ignore.");
                        }
                    }

                    if (this.tokenMetadata.getPendingRanges(keyspaceName, FBUtilities.getBroadcastAddress()).size() > 0) {
                        throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
                    }
                }
            }

            this.startLeaving();
            long timeout = Math.max(RING_DELAY, BatchlogManager.getBatchlogTimeout());
            this.setMode(StorageService.Mode.LEAVING, "sleeping " + timeout + " ms for batch processing and pending range setup", true);
            Thread.sleep(timeout);
            Runnable finishLeaving = new Runnable() {
                public void run() {
                    StorageService.this.shutdownClientServers();
                    Gossiper.instance.stop();

                    try {
                        MessagingService.instance().shutdown();
                    } catch (IOError var2) {
                        StorageService.logger.info("failed to shutdown message service: {}", var2);
                    }

                    StageManager.shutdownNow();
                    StorageService.this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.DECOMMISSIONED);
                    StorageService.this.setMode(StorageService.Mode.DECOMMISSIONED, true);
                }
            };
            this.unbootstrap(finishLeaving);
        } catch (InterruptedException var15) {
            throw new RuntimeException("Node interrupted while decommissioning");
        } catch (ExecutionException var16) {
            logger.error("Error while decommissioning node ", var16.getCause());
            throw new RuntimeException("Error while decommissioning node: " + var16.getCause().getMessage());
        } finally {
            this.isDecommissioning.set(false);
        }

    }

    private static void waitForNodeSyncShutdown(CompletableFuture<Boolean> nodeSyncStopFuture) throws InterruptedException {
        try {
            nodeSyncStopFuture.get(2L, TimeUnit.MINUTES);
        } catch (TimeoutException var2) {
            logger.warn("Wasn't able to stop NodeSync service within 2 minutes during drain. While this generally shouldn't happen (and should be reported if it happens constantly), it should be harmless.");
        } catch (ExecutionException var3) {
            logger.warn("Unexpected error stopping the NodeSync service. This shouldn't happen (and please report) but should be harmless.", var3.getCause());
        }

    }

    private static final Interceptor newGossiperInitGuard() {
        String interceptorName = "Gossiper initialization-guarding interceptor";
        return new AbstractInterceptor("Gossiper initialization-guarding interceptor", ImmutableSet.of(Verbs.SCHEMA.PUSH), Message.Type.all(), ImmutableSet.of(MessageDirection.RECEIVING), ImmutableSet.of(Message.Locality.REMOTE)) {
            protected <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context) {
                StorageService.logger.debug("Message {} intercepted and dropped by {}", message, "Gossiper initialization-guarding interceptor");
                context.drop(message);
            }
        };
    }

    private void leaveRing() {
        this.setBootstrapStateBlocking(SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP);
        this.tokenMetadata.removeEndpoint(FBUtilities.getBroadcastAddress());
        PendingRangeCalculatorService.instance.update();
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, this.valueFactory.left(this.getLocalTokensBlocking(), Gossiper.computeExpireTime()));
        int delay = Math.max(RING_DELAY, 2000);
        logger.info("Announcing that I have left the ring for {}ms", Integer.valueOf(delay));
        Uninterruptibles.sleepUninterruptibly((long) delay, TimeUnit.MILLISECONDS);
    }

    private void unbootstrap(Runnable onFinish) throws ExecutionException, InterruptedException {
        Map<String, Multimap<Range<Token>, InetAddress>> rangesToStream = new HashMap();

        String keyspaceName;
        Multimap rangesMM;
        for (Iterator var3 = Schema.instance.getNonLocalStrategyKeyspaces().iterator(); var3.hasNext(); rangesToStream.put(keyspaceName, rangesMM)) {
            keyspaceName = (String) var3.next();
            rangesMM = this.getChangedRangesForLeaving(keyspaceName, FBUtilities.getBroadcastAddress());
            if (logger.isDebugEnabled()) {
                logger.debug("Ranges needing transfer are [{}]", StringUtils.join(rangesMM.keySet(), ","));
            }
        }

        this.setMode(StorageService.Mode.LEAVING, "replaying batch log and streaming data to other nodes", true);
        Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
        Future<StreamState> streamSuccess = this.streamRangesBlocking(rangesToStream);
        logger.debug("waiting for batch log processing.");
        batchlogReplay.get();
        this.setMode(StorageService.Mode.LEAVING, "streaming hints to other nodes", true);
        Future hintsSuccess = this.streamHints();
        logger.debug("waiting for stream acks.");
        streamSuccess.get();
        hintsSuccess.get();
        logger.debug("stream acks all received.");
        this.leaveRing();
        onFinish.run();
    }

    private Future streamHints() {
        return HintsService.instance.transferHints(this::getPreferredHintsStreamTarget);
    }

    private UUID getPreferredHintsStreamTarget() {
        List<InetAddress> candidates = new ArrayList(instance.getTokenMetadata().cloneAfterAllLeft().getAllEndpoints());
        candidates.remove(FBUtilities.getBroadcastAddress());
        Iterator iter = candidates.iterator();

        while (iter.hasNext()) {
            InetAddress address = (InetAddress) iter.next();
            if (!FailureDetector.instance.isAlive(address)) {
                iter.remove();
            }
        }

        if (candidates.isEmpty()) {
            logger.warn("Unable to stream hints since no live endpoints seen");
            throw new RuntimeException("Unable to stream hints since no live endpoints seen");
        } else {
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), candidates);
            InetAddress hintsDestinationHost = (InetAddress) candidates.get(0);
            return this.tokenMetadata.getHostId(hintsDestinationHost);
        }
    }

    public void move(String newToken) throws IOException {
        try {
            this.getTokenFactory().validate(newToken);
        } catch (ConfigurationException var3) {
            throw new IOException(var3.getMessage());
        }

        this.move(this.getTokenFactory().fromString(newToken));
    }

    private void move(Token newToken) throws IOException {
        if (newToken == null) {
            throw new IOException("Can't move to the undefined (null) token.");
        }
        if (this.tokenMetadata.sortedTokens().contains(newToken)) {
            throw new IOException("target token " + newToken + " is already owned by another node.");
        }
        InetAddress localAddress = FBUtilities.getBroadcastAddress();
        if (this.getTokenMetadata().getTokens(localAddress).size() > 1) {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }
        List<String> keyspacesToProcess = Schema.instance.getNonLocalStrategyKeyspaces();
        PendingRangeCalculatorService.instance.blockUntilFinished();
        for (String keyspaceName : keyspacesToProcess) {
            if (this.tokenMetadata.getPendingRanges(keyspaceName, localAddress).size() <= 0) continue;
            throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, this.valueFactory.moving(newToken));
        this.setMode(Mode.MOVING, String.format("Moving %s from %s to %s.", localAddress, this.getLocalTokensBlocking().iterator().next(), newToken), true);
        this.setMode(Mode.MOVING, String.format("Sleeping %s ms before start streaming/fetching ranges", RING_DELAY), true);
        Uninterruptibles.sleepUninterruptibly((long) RING_DELAY, (TimeUnit) TimeUnit.MILLISECONDS);
        RangeRelocator relocator = new RangeRelocator(Collections.singleton(newToken), keyspacesToProcess);
        if (relocator.streamsNeeded()) {
            this.setMode(Mode.MOVING, "fetching new ranges and streaming old ranges", true);
            try {
                relocator.stream().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Interrupted while waiting for stream/fetch ranges to finish: " + e.getMessage());
            }
        } else {
            this.setMode(Mode.MOVING, "No ranges to fetch/stream", true);
        }
        this.setTokens(Collections.singleton(newToken));
        if (logger.isDebugEnabled()) {
            logger.debug("Successfully moved to new token {}", (Object) this.getLocalTokensBlocking().iterator().next());
        }
    }

    public String getRemovalStatus() {
        return this.removingNode == null ? "No token removals in process." : String.format("Removing token (%s). Waiting for replication confirmation from [%s].", new Object[]{this.tokenMetadata.getToken(this.removingNode), StringUtils.join(this.replicatingNodes, ",")});
    }

    public void forceRemoveCompletion() {
        if (this.replicatingNodes.isEmpty() && this.tokenMetadata.getSizeOfLeavingEndpoints() <= 0) {
            logger.warn("No nodes to force removal on, call 'removenode' first");
        } else {
            logger.warn("Removal not confirmed for for {}", StringUtils.join(this.replicatingNodes, ","));

            for (InetAddress endpoint : this.tokenMetadata.getLeavingEndpoints()) {
                UUID hostId = this.tokenMetadata.getHostId(endpoint);
                Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
                this.excise(this.tokenMetadata.getTokens(endpoint), endpoint);
            }

            this.replicatingNodes.clear();
            this.removingNode = null;
        }

    }

    public void removeNode(final String hostIdString) {
        final InetAddress myAddress = FBUtilities.getBroadcastAddress();
        final UUID localHostId = this.tokenMetadata.getHostId(myAddress);
        final UUID hostId = UUID.fromString(hostIdString);
        final InetAddress endpoint = this.tokenMetadata.getEndpointForHostId(hostId);
        if (endpoint == null) {
            throw new UnsupportedOperationException("Host ID not found.");
        }
        if (!this.tokenMetadata.isMember(endpoint)) {
            throw new UnsupportedOperationException("Node to be removed is not a member of the token ring");
        }
        if (endpoint.equals(myAddress)) {
            throw new UnsupportedOperationException("Cannot remove self");
        }
        if (Gossiper.instance.getLiveMembers().contains(endpoint)) {
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");
        }
        if (this.tokenMetadata.isLeaving(endpoint)) {
            StorageService.logger.warn("Node {} is already being removed, continuing removal anyway", (Object) endpoint);
        }
        if (!this.replicatingNodes.isEmpty()) {
            throw new UnsupportedOperationException("This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");
        }
        final Collection<Token> tokens = this.tokenMetadata.getTokens(endpoint);
        for (final String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces()) {
            if (Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor() == 1) {
                continue;
            }
            final Multimap<Range<Token>, InetAddress> changedRanges = this.getChangedRangesForLeaving(keyspaceName, endpoint);
            final IFailureDetector failureDetector = FailureDetector.instance;
            for (final InetAddress ep : changedRanges.values()) {
                if (failureDetector.isAlive(ep)) {
                    this.replicatingNodes.add(ep);
                } else {
                    StorageService.logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", (Object) ep, (Object) endpoint);
                }
            }
        }
        this.removingNode = endpoint;
        this.tokenMetadata.addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
        Gossiper.instance.advertiseRemoving(endpoint, hostId, localHostId);
        this.restoreReplicaCount(endpoint, myAddress);
        while (!this.replicatingNodes.isEmpty()) {
            Uninterruptibles.sleepUninterruptibly(100L, TimeUnit.MILLISECONDS);
        }
        this.excise(tokens, endpoint);
        Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
        this.replicatingNodes.clear();
        this.removingNode = null;
    }

    public void confirmReplication(InetAddress node) {
        if (!this.replicatingNodes.isEmpty()) {
            this.replicatingNodes.remove(node);
        } else {
            logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
        }

    }

    public String getOperationMode() {
        return this.operationMode.toString();
    }

    public boolean isStarting() {
        return this.operationMode == StorageService.Mode.STARTING;
    }

    public boolean isMoving() {
        return this.operationMode == StorageService.Mode.MOVING;
    }

    public boolean isJoining() {
        return this.operationMode == StorageService.Mode.JOINING;
    }

    public boolean isDrained() {
        return this.operationMode == StorageService.Mode.DRAINED;
    }

    public boolean isDraining() {
        return this.operationMode == StorageService.Mode.DRAINING;
    }

    public String getDrainProgress() {
        return String.format("Drained %s/%s ColumnFamilies", new Object[]{Integer.valueOf(this.remainingCFs), Integer.valueOf(this.totalCFs)});
    }

    public synchronized void drain() throws IOException, InterruptedException, ExecutionException {
        this.drain(false);
    }


    protected synchronized void drain(final boolean isFinalShutdown) throws IOException, InterruptedException, ExecutionException {
        if (this.isShutdown) {
            if (!isFinalShutdown) {
                StorageService.logger.warn("Cannot drain node (did it already happen?)");
            }
            return;
        }
        this.isShutdown = true;
        final Throwable preShutdownHookThrowable = Throwables.perform(null, this.preShutdownHooks.stream().map(h -> h::run));
        if (preShutdownHookThrowable != null) {
            StorageService.logger.error("Attempting to continue draining after pre-shutdown hooks returned exception", preShutdownHookThrowable);
        }
        try {
            this.setMode(Mode.DRAINING, "starting drain process", !isFinalShutdown);
            final CompletableFuture<Boolean> nodeSyncStopFuture = this.nodeSyncService.disableAsync(false);
            waitForNodeSyncShutdown(nodeSyncStopFuture);
            BatchlogManager.instance.shutdown();
            HintsService.instance.pauseDispatch();
            if (this.daemon != null) {
                this.shutdownClientServers();
            }
            ScheduledExecutors.optionalTasks.shutdown();
            Gossiper.instance.stop();
            if (!isFinalShutdown) {
                this.setMode(Mode.DRAINING, "shutting down MessageService", false);
            }
            MessagingService.instance().shutdown();
            if (!isFinalShutdown) {
                this.setMode(Mode.DRAINING, "flushing column families", false);
            }
            for (final Keyspace keyspace2 : Keyspace.all()) {
                for (final ColumnFamilyStore cfs2 : keyspace2.getColumnFamilyStores()) {
                    cfs2.disableAutoCompaction();
                }
            }
            this.totalCFs = 0;
            for (final Keyspace keyspace2 : Keyspace.nonSystem()) {
                this.totalCFs += keyspace2.getColumnFamilyStores().size();
            }
            this.remainingCFs = this.totalCFs;
            try {
                CompletableFutures.allOf(Streams.of(Keyspace.nonSystem()).flatMap(keyspace -> keyspace.getColumnFamilyStores().stream()).map(cfs -> cfs.forceFlush(ColumnFamilyStore.FlushReason.SHUTDOWN).whenComplete((cl, exc) -> --this.remainingCFs))).get(1L, TimeUnit.MINUTES);
            } catch (Throwable t) {
                JVMStabilityInspector.inspectThrowable(t);
                StorageService.logger.error("Caught exception while waiting for memtable flushes during shutdown hook", t);
            }
            CompactionManager.instance.forceShutdown();
            if (SSTableReader.readHotnessTrackerExecutor != null) {
                SSTableReader.readHotnessTrackerExecutor.shutdown();
                if (!SSTableReader.readHotnessTrackerExecutor.awaitTermination(1L, TimeUnit.MINUTES)) {
                    StorageService.logger.warn("Wasn't able to stop the SSTable read hotness tracker with 1 minute.");
                }
            }
            LifecycleTransaction.waitForDeletions();
            try {
                CompletableFutures.allOf(Streams.of(Keyspace.system()).flatMap(keyspace -> keyspace.getColumnFamilyStores().stream()).map(cfs -> cfs.forceFlush(ColumnFamilyStore.FlushReason.SHUTDOWN))).get(1L, TimeUnit.MINUTES);
            } catch (Throwable t) {
                JVMStabilityInspector.inspectThrowable(t);
                StorageService.logger.error("Caught exception while waiting for memtable flushes during shutdown hook", t);
            }
            if (!isFinalShutdown) {
                this.setMode(Mode.DRAINING, "stopping mutations", false);
            }
            final List<OpOrder.Barrier> barriers = StreamSupport.stream(Keyspace.all().spliterator(), false).
                    map(ks -> ks.stopMutations()).collect(Collectors.toList());
            barriers.forEach(OpOrder.Barrier::await);
            if (!isFinalShutdown) {
                this.setMode(Mode.DRAINING, "clearing background IO stage", false);
            }
            StorageProxy.instance.waitForHintsInProgress(3600, TimeUnit.SECONDS);
            HintsService.instance.shutdownBlocking();
            CommitLog.instance.forceRecycleAllSegments();
            CommitLog.instance.shutdownBlocking();
            ScheduledExecutors.nonPeriodicTasks.shutdown();
            if (!ScheduledExecutors.nonPeriodicTasks.awaitTermination(1L, TimeUnit.MINUTES)) {
                StorageService.logger.warn("Failed to wait for non periodic tasks to shutdown");
            }
            ColumnFamilyStore.shutdownPostFlushExecutor();
            ((ParkedThreadsMonitor) ParkedThreadsMonitor.instance.get()).awaitTermination(1L, TimeUnit.MINUTES);
            this.setMode(Mode.DRAINED, !isFinalShutdown);
        } catch (Throwable t2) {
            StorageService.logger.error("Caught an exception while draining ", t2);
        } finally {
            final Throwable postShutdownHookThrowable = Throwables.perform(null, this.postShutdownHooks.stream().map(h -> h::run));
            if (postShutdownHookThrowable != null) {
                StorageService.logger.error("Post-shutdown hooks returned exception", postShutdownHookThrowable);
            }
        }
    }


    public synchronized boolean addPreShutdownHook(Runnable hook) {
        return !this.isDraining() && !this.isDrained() ? this.preShutdownHooks.add(hook) : false;
    }

    public synchronized boolean removePreShutdownHook(Runnable hook) {
        return this.preShutdownHooks.remove(hook);
    }

    public synchronized boolean addPostShutdownHook(Runnable hook) {
        return !this.isDraining() && !this.isDrained() ? this.postShutdownHooks.add(hook) : false;
    }

    public synchronized boolean removePostShutdownHook(Runnable hook) {
        return this.postShutdownHooks.remove(hook);
    }

    synchronized void checkServiceAllowedToStart(String service) {
        if (this.isDraining()) {
            throw new IllegalStateException(String.format("Unable to start %s because the node is draining.", new Object[]{service}));
        } else if (this.isShutdown()) {
            throw new IllegalStateException(String.format("Unable to start %s because the node was drained.", new Object[]{service}));
        }
    }

    @VisibleForTesting
    public IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner) {
        IPartitioner oldPartitioner = DatabaseDescriptor.setPartitionerUnsafe(newPartitioner);
        this.tokenMetadata = this.tokenMetadata.cloneWithNewPartitioner(newPartitioner);
        this.valueFactory = new VersionedValue.VersionedValueFactory(newPartitioner);
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd) {
        TokenMetadata old = this.tokenMetadata;
        this.tokenMetadata = tmd;
        return old;
    }

    public void truncate(String keyspace, String table) throws TimeoutException, IOException {
        this.verifyKeyspaceIsValid(keyspace);

        try {
            StorageProxy.truncateBlocking(keyspace, table);
        } catch (UnavailableException var4) {
            throw new IOException(var4.getMessage());
        }
    }

    public Map<InetAddress, Float> getOwnership() {
        List<Token> sortedTokens = this.tokenMetadata.sortedTokens();
        Map<Token, Float> tokenMap = new TreeMap(this.tokenMetadata.partitioner.describeOwnership(sortedTokens));
        Map<InetAddress, Float> nodeMap = new LinkedHashMap();

        for (Entry<Token, Float> entry : tokenMap.entrySet()) {
            InetAddress endpoint = this.tokenMetadata.getEndpoint((Token) entry.getKey());
            Float tokenOwnership = (Float) entry.getValue();
            if (nodeMap.containsKey(endpoint)) {
                nodeMap.put(endpoint, Float.valueOf(((Float) nodeMap.get(endpoint)).floatValue() + tokenOwnership.floatValue()));
            } else {
                nodeMap.put(endpoint, tokenOwnership);
            }
        }

        return nodeMap;
    }

    public LinkedHashMap<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException {
        AbstractReplicationStrategy strategy;
        if (keyspace != null) {
            Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
            if (keyspaceInstance == null) {
                throw new IllegalArgumentException("The keyspace " + keyspace + ", does not exist");
            }

            if (keyspaceInstance.getReplicationStrategy() instanceof LocalStrategy) {
                throw new IllegalStateException("Ownership values for keyspaces with LocalStrategy are meaningless");
            }

            strategy = keyspaceInstance.getReplicationStrategy();
        } else {
            List<String> userKeyspaces = Schema.instance.getUserKeyspaces();
            if (userKeyspaces.size() > 0) {
                keyspace = (String) userKeyspaces.get(0);
                AbstractReplicationStrategy replicationStrategy = Schema.instance.getKeyspaceInstance(keyspace).getReplicationStrategy();
                for (String keyspaceName : userKeyspaces) {
                    if (!Schema.instance.getKeyspaceInstance(keyspaceName).getReplicationStrategy().hasSameSettings(replicationStrategy)) {
                        throw new IllegalStateException("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
                    }
                }
            } else {
                keyspace = "system_traces";
            }

            Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
            if (keyspaceInstance == null) {
                throw new IllegalArgumentException("The node does not have " + keyspace + " yet, probably still bootstrapping");
            }

            strategy = keyspaceInstance.getReplicationStrategy();
        }

        TokenMetadata metadata = this.tokenMetadata.cloneOnlyTokenMap();
        Collection<Collection<InetAddress>> endpointsGroupedByDc = new ArrayList();
        SortedMap<String, Collection<InetAddress>> sortedDcsToEndpoints = new TreeMap();
        sortedDcsToEndpoints.putAll(metadata.getTopology().getDatacenterEndpoints().asMap());

        sortedDcsToEndpoints.values().forEach(inetAddresses -> {
            endpointsGroupedByDc.add(inetAddresses);
        });

        Map<Token, Float> tokenOwnership = this.tokenMetadata.partitioner.describeOwnership(this.tokenMetadata.sortedTokens());
        LinkedHashMap<InetAddress, Float> finalOwnership = Maps.newLinkedHashMap();
        Multimap<InetAddress, Range<Token>> endpointToRanges = strategy.getAddressRanges();

        for (Collection<InetAddress> endpoints : endpointsGroupedByDc) {
            for (InetAddress endpoint : endpoints) {
                float ownership = 0.0F;
                for (Range<Token> range : endpointToRanges.get(endpoint)) {
                    if (tokenOwnership.containsKey(range.right)) {
                        ownership += ((Float) tokenOwnership.get(range.right)).floatValue();
                    }
                }
                finalOwnership.put(endpoint, Float.valueOf(ownership));
            }
        }

        return finalOwnership;
    }

    public List<String> getKeyspaces() {
        List<String> keyspaceNamesList = new ArrayList(Schema.instance.getKeyspaces());
        return Collections.unmodifiableList(keyspaceNamesList);
    }

    public List<String> getNonSystemKeyspaces() {
        List<String> nonKeyspaceNamesList = new ArrayList(Schema.instance.getNonSystemKeyspaces());
        return UnmodifiableArrayList.copyOf((Collection) nonKeyspaceNamesList);
    }

    public List<String> getNonLocalStrategyKeyspaces() {
        return Collections.unmodifiableList(Schema.instance.getNonLocalStrategyKeyspaces());
    }

    public Map<String, Map<String, String>> getTableInfos(String keyspace, String... tables) {
        HashMap tableInfos = new HashMap();

        try {
            this.getValidColumnFamilies(false, false, keyspace, tables).forEach((cfs) -> {
                tableInfos.put(cfs.name, cfs.getTableInfo().asMap());
            });
            return tableInfos;
        } catch (IOException var5) {
            throw new RuntimeException(String.format("Could not retrieve info for keyspace %s and table(s) %s.", new Object[]{keyspace, tables}), var5);
        }
    }

    public Map<String, List<String>> getKeyspacesAndViews() {
        Map<String, List<String>> map = new HashMap();

        for (String ks : Schema.instance.getKeyspaces()) {
            List<String> tables = new ArrayList();
            map.put(ks, tables);
            Schema.instance.getKeyspaceMetadata(ks).views.forEach(viewMetadata -> {
                tables.add(viewMetadata.name);
            });
        }

        return map;
    }

    public Map<String, String> getViewBuildStatuses(String keyspace, String view) {
        Map<UUID, String> coreViewStatus = (Map) TPCUtils.blockingGet(SystemDistributedKeyspace.viewStatus(keyspace, view));
        Map<InetAddress, UUID> hostIdToEndpoint = this.tokenMetadata.getEndpointToHostIdMapForReading();
        Map<String, String> result = new HashMap();

        for (Entry<InetAddress, UUID> entry : hostIdToEndpoint.entrySet()) {
            UUID hostId = entry.getValue();
            InetAddress endpoint = entry.getKey();
            result.put(endpoint.toString(), coreViewStatus.containsKey(hostId) ? (String) coreViewStatus.get(hostId) : "UNKNOWN");
        }

        return Collections.unmodifiableMap(result);
    }

    public void setDynamicUpdateInterval(int dynamicUpdateInterval) {
        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch) {
            try {
                this.updateSnitch((String) null, Boolean.valueOf(true), Integer.valueOf(dynamicUpdateInterval), (Integer) null, (Double) null);
            } catch (ClassNotFoundException var3) {
                throw new RuntimeException(var3);
            }
        }

    }

    public int getDynamicUpdateInterval() {
        return DatabaseDescriptor.getDynamicUpdateInterval();
    }

    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException {
        if (dynamicUpdateInterval != null) {
            DatabaseDescriptor.setDynamicUpdateInterval(dynamicUpdateInterval.intValue());
        }

        if (dynamicResetInterval != null) {
            DatabaseDescriptor.setDynamicResetInterval(dynamicResetInterval.intValue());
        }

        if (dynamicBadnessThreshold != null) {
            DatabaseDescriptor.setDynamicBadnessThreshold(dynamicBadnessThreshold.doubleValue());
        }

        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        if (epSnitchClassName != null) {
            if (oldSnitch instanceof DynamicEndpointSnitch) {
                ((DynamicEndpointSnitch) oldSnitch).close();
            }

            IEndpointSnitch newSnitch;
            try {
                newSnitch = DatabaseDescriptor.createEndpointSnitch(dynamic != null && dynamic.booleanValue(), epSnitchClassName);
            } catch (ConfigurationException var10) {
                throw new ClassNotFoundException(var10.getMessage());
            }

            if (newSnitch instanceof DynamicEndpointSnitch) {
                logger.info("Created new dynamic snitch {} with update-interval={}, reset-interval={}, badness-threshold={}", new Object[]{((DynamicEndpointSnitch) newSnitch).subsnitch.getClass().getName(), Integer.valueOf(DatabaseDescriptor.getDynamicUpdateInterval()), Integer.valueOf(DatabaseDescriptor.getDynamicResetInterval()), Double.valueOf(DatabaseDescriptor.getDynamicBadnessThreshold())});
            } else {
                logger.info("Created new non-dynamic snitch {}", newSnitch.getClass().getName());
            }

            DatabaseDescriptor.setEndpointSnitch(newSnitch);

            String ks;
            for (Iterator var8 = Schema.instance.getKeyspaces().iterator(); var8.hasNext(); Keyspace.open(ks).getReplicationStrategy().snitch = newSnitch) {
                ks = (String) var8.next();
            }
        } else if (oldSnitch instanceof DynamicEndpointSnitch) {
            logger.info("Applying config change to dynamic snitch {} with update-interval={}, reset-interval={}, badness-threshold={}", new Object[]{((DynamicEndpointSnitch) oldSnitch).subsnitch.getClass().getName(), Integer.valueOf(DatabaseDescriptor.getDynamicUpdateInterval()), Integer.valueOf(DatabaseDescriptor.getDynamicResetInterval()), Double.valueOf(DatabaseDescriptor.getDynamicBadnessThreshold())});
            DynamicEndpointSnitch snitch = (DynamicEndpointSnitch) oldSnitch;
            snitch.applyConfigChanges();
        }

        this.updateTopology();
    }

    public String getBatchlogEndpointStrategy() {
        return DatabaseDescriptor.getBatchlogEndpointStrategy().name();
    }

    public void setBatchlogEndpointStrategy(String batchlogEndpointStrategy) {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.valueOf(batchlogEndpointStrategy));
    }

    private Future<StreamState> streamRangesBlocking(Map<String, Multimap<Range<Token>, InetAddress>> rangesToStreamByKeyspace) {
        HashMap<String, Map<InetAddress, List<Range>>> sessionsToStreamByKeyspace = new HashMap();
        for (Map.Entry<String, Multimap<Range<Token>, InetAddress>> entry : rangesToStreamByKeyspace.entrySet()) {
            String keyspace = entry.getKey();
            Multimap<Range<Token>, InetAddress> rangesWithEndpoints = entry.getValue();
            if (rangesWithEndpoints.isEmpty()) continue;
            Map<InetAddress, Set<Range<Token>>> transferredRangePerKeyspace = TPCUtils.blockingGet(SystemKeyspace.getTransferredRanges("Unbootstrap", keyspace, StorageService.instance.getTokenMetadata().partitioner));
            HashMap<InetAddress, List<Range>> rangesPerEndpoint = new HashMap();
            for (Map.Entry endPointEntry : rangesWithEndpoints.entries()) {
                Range range = (Range) endPointEntry.getKey();
                InetAddress endpoint = (InetAddress) endPointEntry.getValue();
                Set<Range<Token>> transferredRanges = transferredRangePerKeyspace.get(endpoint);
                if (transferredRanges != null && transferredRanges.contains(range)) {
                    logger.debug("Skipping transferred range {} of keyspace {}, endpoint {}", new Object[]{range, keyspace, endpoint});
                    continue;
                }
                List<Range> curRanges = rangesPerEndpoint.get(endpoint);
                if (curRanges == null) {
                    curRanges = new LinkedList<Range>();
                    rangesPerEndpoint.put(endpoint, curRanges);
                }
                curRanges.add(range);
            }
            sessionsToStreamByKeyspace.put(keyspace, rangesPerEndpoint);
        }
        StreamPlan streamPlan = new StreamPlan(StreamOperation.DECOMMISSION, true, true);
        streamPlan.listeners(this.streamStateStore, new StreamEventHandler[0]);
        for (Map.Entry<String, Map<InetAddress, List<Range>>> entry : sessionsToStreamByKeyspace.entrySet()) {
            String keyspaceName = entry.getKey();
            for (Map.Entry<InetAddress, List<Range>> rangesEntry : entry.getValue().entrySet()) {
                List ranges = rangesEntry.getValue();
                InetAddress newEndpoint = rangesEntry.getKey();
                InetAddress preferred = SystemKeyspace.getPreferredIP(newEndpoint);
                streamPlan.transferRanges(newEndpoint, preferred, keyspaceName, ranges);
            }
        }
        return streamPlan.execute();
    }

    public Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(Collection<Range<Token>> current, Collection<Range<Token>> updated) {
        boolean intersect;
        Set toStream = SetsFactory.newSet();
        Set toFetch = SetsFactory.newSet();
        for (Range<Token> r1 : current) {
            intersect = false;
            for (Range<Token> r2 : updated) {
                if (!r1.intersects(r2)) continue;
                toStream.addAll(r1.subtract(r2));
                intersect = true;
            }
            if (intersect) continue;
            toStream.add(r1);
        }
        for (Range<Token> r2 : updated) {
            intersect = false;
            for (Range<Token> r1 : current) {
                if (!r2.intersects(r1)) continue;
                toFetch.addAll(r2.subtract(r1));
                intersect = true;
            }
            if (intersect) continue;
            toFetch.add(r2);
        }
        return Pair.create(toStream, toFetch);
    }

    public void bulkLoad(String directory) {
        try {
            this.bulkLoadInternal(directory).get();
        } catch (Exception var3) {
            throw new RuntimeException(var3);
        }
    }

    public String bulkLoadAsync(String directory) {
        return this.bulkLoadInternal(directory).planId.toString();
    }

    private StreamResultFuture bulkLoadInternal(String directory) {
        File dir = new File(directory);
        if (dir.exists() && dir.isDirectory()) {
            SSTableLoader.Client client = new SSTableLoader.Client() {
                private String keyspace;

                public void init(String keyspace) {
                    this.keyspace = keyspace;

                    try {
                        for (Entry<Range<Token>, List<InetAddress>> entry : StorageService.instance.getRangeToAddressMap(keyspace).entrySet()) {
                            Range<Token> range = entry.getKey();
                            for (InetAddress endpoint : entry.getValue()) {
                                this.addRangeForEndpoint(range, endpoint);
                            }
                        }

                    } catch (Exception var7) {
                        throw new RuntimeException(var7);
                    }
                }

                public TableMetadataRef getTableMetadata(String tableName) {
                    return Schema.instance.getTableMetadataRef(this.keyspace, tableName);
                }
            };
            return (new SSTableLoader(dir, client, new OutputHandler.LogOutput())).stream();
        } else {
            throw new IllegalArgumentException("Invalid directory " + directory);
        }
    }

    public void rescheduleFailedDeletions() {
        LifecycleTransaction.rescheduleFailedDeletions();
    }

    public void loadNewSSTables(String ksName, String cfName, boolean resetLevels) {
        if (!this.isInitialized()) {
            throw new RuntimeException("Not yet initialized, can't load new sstables");
        } else {
            this.verifyKeyspaceIsValid(ksName);
            ColumnFamilyStore.loadNewSSTables(ksName, cfName, resetLevels);
        }
    }

    public List<String> sampleKeyRange() {
        List<DecoratedKey> keys = new ArrayList();

        for (Keyspace keyspace : Keyspace.nonLocalStrategy()) {
            for (Range<Token> range : this.getPrimaryRangesForEndpoint(keyspace.getName(), FBUtilities.getBroadcastAddress())) {
                keys.addAll(this.keySamples(keyspace.getColumnFamilyStores(), range));
            }
        }

        List<String> sampledKeys = new ArrayList(keys.size());

        for (DecoratedKey key : keys) {
            sampledKeys.add(key.getToken().toString());
        }

        return sampledKeys;
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames) {
        String[] indices = (String[]) ((List) Arrays.asList(idxNames).stream().map((p) -> {
            return SecondaryIndexManager.isIndexColumnFamily(p) ? SecondaryIndexManager.getIndexName(p) : p;
        }).collect(Collectors.toList())).toArray(new String[0]);
        ColumnFamilyStore.rebuildSecondaryIndex(ksName, cfName, indices);
    }

    public void resetLocalSchema() throws IOException {
        MigrationManager.resetLocalSchema();
    }

    public void reloadLocalSchema() {
        Schema.instance.reloadSchemaAndAnnounceVersion();
    }

    public void setTraceProbability(double probability) {
        this.traceProbability = probability;
    }

    public double getTraceProbability() {
        return this.traceProbability;
    }

    public void disableAutoCompaction(String ks, String... tables) throws IOException {
        for (ColumnFamilyStore cfs : this.getValidColumnFamilies(true, true, ks, tables)) {
            cfs.disableAutoCompaction();
        }
    }

    public synchronized void enableAutoCompaction(String ks, String... tables) throws IOException {
        this.checkServiceAllowedToStart("auto compaction");

        for (ColumnFamilyStore cfs : this.getValidColumnFamilies(true, true, ks, tables)) {
            cfs.enableAutoCompaction();
        }
    }

    public Map<String, Boolean> getAutoCompactionStatus(String ks, String... tables) throws IOException {
        Map<String, Boolean> status = new HashMap();

        for (ColumnFamilyStore cfs : this.getValidColumnFamilies(true, true, ks, tables)) {
            status.put(cfs.getTableName(), Boolean.valueOf(cfs.isAutoCompactionDisabled()));
        }

        return status;
    }

    public String getClusterName() {
        return DatabaseDescriptor.getClusterName();
    }

    public String getPartitionerName() {
        return DatabaseDescriptor.getPartitionerName();
    }

    public int getTombstoneWarnThreshold() {
        return DatabaseDescriptor.getTombstoneWarnThreshold();
    }

    public void setTombstoneWarnThreshold(int threshold) {
        DatabaseDescriptor.setTombstoneWarnThreshold(threshold);
    }

    public int getTombstoneFailureThreshold() {
        return DatabaseDescriptor.getTombstoneFailureThreshold();
    }

    public void setTombstoneFailureThreshold(int threshold) {
        DatabaseDescriptor.setTombstoneFailureThreshold(threshold);
    }

    public int getBatchSizeFailureThreshold() {
        return DatabaseDescriptor.getBatchSizeFailThresholdInKB();
    }

    public void setBatchSizeFailureThreshold(int threshold) {
        DatabaseDescriptor.setBatchSizeFailThresholdInKB(threshold);
        logger.info("Updated batch_size_fail_threshold_in_kb to {}", Integer.valueOf(threshold));
    }

    public int getBatchSizeWarnThreshold() {
        return DatabaseDescriptor.getBatchSizeWarnThresholdInKB();
    }

    public void setBatchSizeWarnThreshold(int threshold) {
        DatabaseDescriptor.setBatchSizeWarnThresholdInKB(threshold);
        logger.info("Updated batch_size_warn_threshold_in_kb to {}", Integer.valueOf(threshold));
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB) {
        DatabaseDescriptor.setHintedHandoffThrottleInKB(throttleInKB);
        logger.info("Updated hinted_handoff_throttle_in_kb to {}", Integer.valueOf(throttleInKB));
    }

    public long getPid() {
        return NativeLibrary.getProcessID();
    }

    public static List<Range<Token>> getStartupTokenRanges(Keyspace keyspace) {
        if (!DatabaseDescriptor.getPartitioner().splitter().isPresent()) {
            return null;
        } else {
            Object lr;
            if (instance.isBootstrapMode()) {
                lr = instance.getTokenMetadata().getPendingRanges(keyspace.getName(), FBUtilities.getBroadcastAddress());
            } else {
                TokenMetadata tmd = instance.getTokenMetadata().cloneAfterAllSettled();
                lr = keyspace.getReplicationStrategy().getAddressRanges(tmd).get(FBUtilities.getBroadcastAddress());
            }

            return lr != null && !((Collection) lr).isEmpty() ? Range.sort((Collection) lr) : null;
        }
    }

    public int forceMarkAllSSTablesAsUnrepaired(String keyspace, String... tables) throws IOException {
        int marked = 0;

        for (ColumnFamilyStore cfs : this.getValidColumnFamilies(false, false, keyspace, tables)) {
            try {
                marked += cfs.forceMarkAllSSTablesAsUnrepaired();
            } catch (Throwable var7) {
                logger.error("Error while marking all SSTables from table {}.{} as unrepaired. Please trigger operation again or manually mark SSTables as unrepaired otherwise rows already purged on other replicas may be propagated to other replicas during incremental repair without their respectives tombstones.", new Object[]{keyspace, cfs.name, var7});
                throw new RuntimeException(var7);
            }
        }

        return marked;
    }

    public boolean shouldTraceRequest() {
        double traceProbability = this.getTraceProbability();
        return traceProbability != 0.0D && ThreadLocalRandom.current().nextDouble() < traceProbability;
    }

    static {
        joinRing = PropertyConfiguration.PUBLIC.getBoolean("cassandra.join_ring", true);
    }

    private class RangeRelocator {
        private final StreamPlan streamPlan;

        private RangeRelocator(Collection<Token> tokens, List<String> keyspaceNames) {
            this.streamPlan = new StreamPlan(StreamOperation.RELOCATION, true, true);
            this.calculateToFromStreams(tokens, keyspaceNames);
        }

        private void calculateToFromStreams(Collection<Token> newTokens, List<String> keyspaceNames) {
            InetAddress localAddress = FBUtilities.getBroadcastAddress();
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            TokenMetadata tokenMetaCloneAllSettled = StorageService.this.tokenMetadata.cloneAfterAllSettled();
            TokenMetadata tokenMetaClone = StorageService.this.tokenMetadata.cloneOnlyTokenMap();
            for (String keyspace : keyspaceNames) {
                AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
                Multimap<InetAddress, Range<Token>> endpointToRanges = strategy.getAddressRanges();
                logger.debug("Calculating ranges to stream and request for keyspace {}", (Object) keyspace);
                for (Token newToken : newTokens) {
                    Collection currentRanges = endpointToRanges.get(localAddress);
                    Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);
                    Multimap<Range<Token>, InetAddress> rangeAddresses = strategy.getRangeAddresses(tokenMetaClone);
                    Pair<Set<Range<Token>>, Set<Range<Token>>> rangesPerKeyspace = StorageService.this.calculateStreamAndFetchRanges(currentRanges, updatedRanges);
                    ArrayListMultimap rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
                    for (Range<Token> rangeTo : rangesPerKeyspace.right) {
                        for (Range<Token> range : rangeAddresses.keySet()) {
                            if (!range.contains(rangeTo)) continue;
                            List endpoints = null;
                            if (useStrictConsistency) {
                                final Set<InetAddress> oldEndpoints = (Set<InetAddress>) Sets.newHashSet((Iterable) rangeAddresses.get(range));
                                final Set<InetAddress> newEndpoints = (Set<InetAddress>) Sets.newHashSet((Iterable) strategy.calculateNaturalEndpoints(rangeTo.right, tokenMetaCloneAllSettled));
                                if (oldEndpoints.size() == strategy.getReplicationFactor()) {
                                    oldEndpoints.removeAll(newEndpoints);
                                    if (oldEndpoints.isEmpty()) continue;
                                    assert (oldEndpoints.size() == 1);
                                }
                                endpoints = Lists.newArrayList((Object[]) new InetAddress[]{(InetAddress) oldEndpoints.iterator().next()});
                            } else {
                                endpoints = snitch.getSortedListByProximity(localAddress, rangeAddresses.get(range));
                            }
                            rangesToFetchWithPreferredEndpoints.putAll(rangeTo, endpoints);
                        }
                        Collection addressList = rangesToFetchWithPreferredEndpoints.get(rangeTo);
                        if (addressList == null || addressList.isEmpty() || !useStrictConsistency) continue;
                        if (addressList.size() > 1) {
                            throw new IllegalStateException("Multiple strict sources found for " + rangeTo);
                        }
                        InetAddress sourceIp = (InetAddress) addressList.iterator().next();
                        if (!Gossiper.instance.isEnabled() || Gossiper.instance.getEndpointStateForEndpoint(sourceIp).isAlive())
                            continue;
                        throw new RuntimeException("A node required to move the data consistently is down (" + sourceIp + ").  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
                    }
                    HashMultimap<InetAddress, Range<Token>> endpointRanges = HashMultimap.create();
                    for (Range<Token> toStream : rangesPerKeyspace.left) {
                        ImmutableSet<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token) toStream.right, tokenMetaClone));
                        ImmutableSet<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints((Token) toStream.right, tokenMetaCloneAllSettled));
                        logger.debug("Range: {} Current endpoints: {} New endpoints: {}", new Object[]{toStream, currentEndpoints, newEndpoints});
                        for (InetAddress address : Sets.difference(newEndpoints, currentEndpoints)) {
                            logger.debug("Range {} has new owner {}", (Object) toStream, (Object) address);
                            endpointRanges.put(address, toStream);
                        }
                    }
                    for (InetAddress address : endpointRanges.keySet()) {
                        logger.debug("Will stream range {} of keyspace {} to endpoint {}", new Object[]{endpointRanges.get(address), keyspace, address});
                        InetAddress preferred = SystemKeyspace.getPreferredIP(address);
                        this.streamPlan.transferRanges(address, preferred, keyspace, endpointRanges.get(address));
                    }
                    Multimap<InetAddress, Range<Token>> workMap = RangeStreamer.getWorkMapForMove((Multimap<Range<Token>, InetAddress>) rangesToFetchWithPreferredEndpoints, keyspace, FailureDetector.instance, useStrictConsistency);
                    for (InetAddress address : workMap.keySet()) {
                        logger.debug("Will request range {} of keyspace {} from endpoint {}", new Object[]{workMap.get(address), keyspace, address});
                        InetAddress preferred = SystemKeyspace.getPreferredIP(address);
                        this.streamPlan.requestRanges(address, preferred, keyspace, workMap.get(address));
                    }
                    logger.debug("Keyspace {}: work map {}.", (Object) keyspace, workMap);
                }
            }
        }


        public Future<StreamState> stream() {
            return this.streamPlan.execute();
        }

        public boolean streamsNeeded() {
            return !this.streamPlan.isEmpty();
        }
    }

    private static enum Mode {
        STARTING,
        NORMAL,
        JOINING,
        LEAVING,
        DECOMMISSIONED,
        MOVING,
        DRAINING,
        DRAINED;

        private Mode() {
        }
    }
}
