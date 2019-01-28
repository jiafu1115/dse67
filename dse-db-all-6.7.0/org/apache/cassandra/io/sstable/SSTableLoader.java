package org.apache.cassandra.io.sstable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.function.BiPredicate;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.concurrent.Ref;

public class SSTableLoader implements StreamEventHandler {
    private final File directory;
    private final String keyspace;
    private final SSTableLoader.Client client;
    private final int connectionsPerHost;
    private final OutputHandler outputHandler;
    private final Set<InetAddress> failedHosts;
    private final List<SSTableReader> sstables;
    private final Multimap<InetAddress, StreamSession.SSTableStreamingSections> streamingDetails;

    public SSTableLoader(File directory, SSTableLoader.Client client, OutputHandler outputHandler) {
        this(directory, client, outputHandler, 1);
    }

    public SSTableLoader(File directory, SSTableLoader.Client client, OutputHandler outputHandler, int connectionsPerHost) {
        this.failedHosts = SetsFactory.newSet();
        this.sstables = new ArrayList();
        this.streamingDetails = HashMultimap.create();
        this.directory = directory;
        this.keyspace = directory.getParentFile().getName();
        this.client = client;
        this.outputHandler = outputHandler;
        this.connectionsPerHost = connectionsPerHost;
    }

    protected Collection<SSTableReader> openSSTables(Map<InetAddress, Collection<Range<Token>>> ranges) {
        this.outputHandler.output("Opening sstables and calculating sections to stream");
        LifecycleTransaction.getFiles(this.directory.toPath(), (file, type) -> {
            String name = file.getName();
            if (type != Directories.FileType.FINAL) {
                this.outputHandler.output(String.format("Skipping temporary file %s", new Object[]{name}));
                return false;
            }
            Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(file);
            Descriptor desc = p == null ? null : (Descriptor) p.left;
            if (p != null && ((Component) p.right).equals(Component.DATA)) {
                Set<Component> components = mainComponentsPresent(desc);
                TableMetadataRef metadata = this.client.getTableMetadata(desc.cfname);
                if (metadata == null) {
                    this.outputHandler.output(String.format("Skipping file %s: table %s.%s doesn't exist", new Object[]{name, this.keyspace, desc.cfname}));
                    return false;
                }
                try {
                    SSTableReader sstable = SSTableReader.openForBatch(desc, components, metadata);
                    this.sstables.add(sstable);

                    for (Entry<InetAddress, Collection<Range<Token>>> entry : ranges.entrySet()) {
                        InetAddress endpoint = (InetAddress) entry.getKey();
                        Collection<Range<Token>> tokenRanges = (Collection) entry.getValue();
                        List<Pair<Long, Long>> sstableSections = sstable.getPositionsForRanges(tokenRanges);
                        long estimatedKeys = sstable.estimatedKeysForRanges(tokenRanges);
                        Ref<SSTableReader> ref = sstable.ref();
                        StreamSession.SSTableStreamingSections details = new StreamSession.SSTableStreamingSections(ref, sstableSections, estimatedKeys);
                        this.streamingDetails.put(endpoint, details);
                    }
                } catch (FSError var19) {
                    this.outputHandler.output(String.format("Skipping file %s, error opening it: %s", new Object[]{name, var19.getMessage()}));
                }

                return false;
            } else {
                return false;
            }
        }, Directories.OnTxnErr.IGNORE);
        return this.sstables;
    }

    public static Set<Component> mainComponentsPresent(Descriptor desc) {
        Sets.SetView<Component> lookFor = Sets.union(SSTableReader.requiredComponents(desc), (Set) ImmutableSet.of((Object) Component.COMPRESSION_INFO));
        Set<Component> components = SetsFactory.newSet();
        for (Component component : lookFor) {
            if (!new File(desc.filenameFor(component)).exists()) continue;
            components.add(component);
        }
        return components;
    }

    public StreamResultFuture stream() {
        return this.stream(Collections.emptySet(), new StreamEventHandler[0]);
    }


    public /* varargs */ StreamResultFuture stream(Set<InetAddress> toIgnore, StreamEventHandler... listeners) {
        this.client.init(this.keyspace);
        this.outputHandler.output("Established connection to initial hosts");
        StreamPlan plan = new StreamPlan(StreamOperation.BULK_LOAD, this.connectionsPerHost, false, false, null, PreviewKind.NONE).connectionFactory(this.client.getConnectionFactory());
        Map<InetAddress, Collection<Range<Token>>> endpointToRanges = this.client.getEndpointToRangesMap();
        this.openSSTables(endpointToRanges);
        if (this.sstables.isEmpty()) {
            return plan.execute();
        }
        this.outputHandler.output(String.format("Streaming relevant part of %s to %s", this.names(this.sstables), endpointToRanges.keySet()));
        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : endpointToRanges.entrySet()) {
            InetAddress remote = entry.getKey();
            if (toIgnore.contains(remote)) continue;
            LinkedList<StreamSession.SSTableStreamingSections> endpointDetails = new LinkedList<StreamSession.SSTableStreamingSections>();
            for (StreamSession.SSTableStreamingSections details : this.streamingDetails.get(remote)) {
                endpointDetails.add(details);
            }
            plan.transferFiles(remote, endpointDetails);
        }
        plan.listeners(this, listeners);
        return plan.execute();
    }

    public void onSuccess(StreamState finalState) {
        this.releaseReferences();
    }

    public void onFailure(Throwable t) {
        this.releaseReferences();
    }

    private void releaseReferences() {
        for (SSTableReader sstable : this.sstables) {
            sstable.selfRef().release();
            assert (sstable.selfRef().globalCount() == 0);
        }
    }

    public void handleStreamEvent(StreamEvent event) {
        if (event.eventType == StreamEvent.Type.STREAM_COMPLETE) {
            StreamEvent.SessionCompleteEvent se = (StreamEvent.SessionCompleteEvent) event;
            if (!se.success) {
                this.failedHosts.add(se.peer);
            }
        }

    }

    private String names(Collection<SSTableReader> sstables) {
        StringBuilder builder = new StringBuilder();
        Iterator var3 = sstables.iterator();

        while (var3.hasNext()) {
            SSTableReader sstable = (SSTableReader) var3.next();
            builder.append(sstable.descriptor.filenameFor(Component.DATA)).append(" ");
        }

        return builder.toString();
    }

    public Set<InetAddress> getFailedHosts() {
        return this.failedHosts;
    }

    public abstract static class Client {
        private final Map<InetAddress, Collection<Range<Token>>> endpointToRanges = new HashMap();

        public Client() {
        }

        public abstract void init(String var1);

        public void stop() {
        }

        public StreamConnectionFactory getConnectionFactory() {
            return new DefaultConnectionFactory();
        }

        public abstract TableMetadataRef getTableMetadata(String var1);

        public void setTableMetadata(TableMetadataRef cfm) {
            throw new RuntimeException();
        }

        public Map<InetAddress, Collection<Range<Token>>> getEndpointToRangesMap() {
            return this.endpointToRanges;
        }

        protected void addRangeForEndpoint(Range<Token> range, InetAddress endpoint) {
            Collection<Range<Token>> ranges = (Collection) this.endpointToRanges.get(endpoint);
            if (ranges == null) {
                ranges = SetsFactory.newSet();
                this.endpointToRanges.put(endpoint, ranges);
            }

            ((Collection) ranges).add(range);
        }
    }
}
