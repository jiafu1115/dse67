package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.datastax.bdp.insights.events.GossipChangeInformation;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class GossipRuntimeManager implements InsightsRuntimeConfigComponent {
   private static final Logger logger = LoggerFactory.getLogger(GossipRuntimeManager.class);
   private final AtomicBoolean started = new AtomicBoolean(false);
   private final IEndpointStateChangeSubscriber gossipListener;

   @Inject
   public GossipRuntimeManager(InsightsClient client) {
      this.gossipListener = new GossipRuntimeManager.InsightGossipChangeListener(client);
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         Gossiper.instance.register(this.gossipListener);
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         Gossiper.instance.unregister(this.gossipListener);
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public boolean shouldRestart(InsightsRuntimeConfig oldConfig, InsightsRuntimeConfig newConfig) {
      return false;
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.gossip_change");
   }

   static class InsightGossipChangeListener implements IEndpointStateChangeSubscriber {
      private final InsightsClient client;

      InsightGossipChangeListener(InsightsClient client) {
         this.client = client;
      }

      public void onJoin(InetAddress endpoint, EndpointState epState) {
         try {
            this.client.report(new GossipChangeInformation(GossipChangeInformation.GossipEventType.JOINED, endpoint, epState));
         } catch (Exception var4) {
            GossipRuntimeManager.logger.warn("Error reporting gossip info to Insights", var4);
         }

      }

      public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
      }

      public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      }

      public void onAlive(InetAddress endpoint, EndpointState state) {
         try {
            this.client.report(new GossipChangeInformation(GossipChangeInformation.GossipEventType.ALIVE, endpoint, state));
         } catch (Exception var4) {
            GossipRuntimeManager.logger.warn("Error reporting gossip info to Insights", var4);
         }

      }

      public void onDead(InetAddress endpoint, EndpointState state) {
         try {
            this.client.report(new GossipChangeInformation(GossipChangeInformation.GossipEventType.DEAD, endpoint, state));
         } catch (Exception var4) {
            GossipRuntimeManager.logger.warn("Error reporting gossip info to Insights", var4);
         }

      }

      public void onRemove(InetAddress endpoint) {
         try {
            this.client.report(new GossipChangeInformation(GossipChangeInformation.GossipEventType.REMOVED, endpoint, (EndpointState)null));
         } catch (Exception var3) {
            GossipRuntimeManager.logger.warn("Error reporting gossip info to Insights", var3);
         }

      }

      public void onRestart(InetAddress endpoint, EndpointState state) {
         try {
            this.client.report(new GossipChangeInformation(GossipChangeInformation.GossipEventType.RESTARTED, endpoint, state));
         } catch (Exception var4) {
            GossipRuntimeManager.logger.warn("Error reporting gossip info to Insights", var4);
         }

      }
   }
}
