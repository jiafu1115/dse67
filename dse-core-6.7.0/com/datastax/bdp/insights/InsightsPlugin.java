package com.datastax.bdp.insights;

import com.datastax.bdp.insights.reporting.CompactionRuntimeManager;
import com.datastax.bdp.insights.reporting.DroppedMessageRuntimeManager;
import com.datastax.bdp.insights.reporting.ExceptionRuntimeManager;
import com.datastax.bdp.insights.reporting.FlushRuntimeManager;
import com.datastax.bdp.insights.reporting.GCInformationRuntimeManager;
import com.datastax.bdp.insights.reporting.GossipRuntimeManager;
import com.datastax.bdp.insights.reporting.NodeLocalInsightsRuntimeManager;
import com.datastax.bdp.insights.reporting.SchemaChangeRuntimeManager;
import com.datastax.bdp.insights.rpc.InsightsRpc;
import com.datastax.bdp.insights.storage.config.FilteringRule;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfigManager;
import com.datastax.bdp.insights.storage.schema.InsightsConfigChangeListener;
import com.datastax.bdp.plugin.AbstractPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.DseSystemPlugin;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {DseSystemPlugin.class, ThreadPoolPlugin.class}
)
public final class InsightsPlugin extends AbstractPlugin implements InsightsConfigChangeListener {
   private static final Logger logger = LoggerFactory.getLogger(InsightsPlugin.class);
   private final InsightsRuntimeConfigManager runtimeConfigManager;
   private final InsightsRpc insightsRpc;
   private final ImmutableList<InsightsRuntimeConfigComponent> components;

   @Inject
   public InsightsPlugin(InsightsClientRuntimeManager clientRuntimeManager, NodeLocalInsightsRuntimeManager nodeLocalInsightsRuntimeManager, GCInformationRuntimeManager gcInformationRuntimeManager, CompactionRuntimeManager compactionRuntimeManager, InsightsRuntimeConfigManager runtimeConfigManager, GossipRuntimeManager gossipRuntimeManager, FlushRuntimeManager flushRuntimeManager, ExceptionRuntimeManager exceptionRuntimeManager, SchemaChangeRuntimeManager schemaChangeRuntimeManager, DroppedMessageRuntimeManager droppedMessageRuntimeManager, InsightsRpc insightsRpc) {
      this.runtimeConfigManager = runtimeConfigManager;
      this.insightsRpc = insightsRpc;
      this.components = ImmutableList.of(nodeLocalInsightsRuntimeManager, clientRuntimeManager, gcInformationRuntimeManager, compactionRuntimeManager, gossipRuntimeManager, flushRuntimeManager, exceptionRuntimeManager, schemaChangeRuntimeManager, droppedMessageRuntimeManager);
      runtimeConfigManager.addConfigChangeListener(this);
      runtimeConfigManager.addConfigChangeListener(clientRuntimeManager);
   }

   public boolean isEnabled() {
      return true;
   }

   public void setupSchema() {
   }

   public void onActivate() {
      this.runtimeConfigManager.start();
      InsightsRuntimeConfig config = this.runtimeConfigManager.getRuntimeConfig();
      this.enableInsightsComponentsIfNecessary(config, config);
      this.maybeEnableRpc(config);
   }

   private void maybeEnableRpc(InsightsRuntimeConfig config) {
      FilteringRule rule = FilteringRule.applyFilters("dse.insights.event.driver_rpc", config.config.filtering_rules);
      if(config.isEnabled() && rule.isAllowRule) {
         this.insightsRpc.setAllowed();
      } else {
         this.insightsRpc.setFiltered();
      }

   }

   public void onRegister() {
      RpcRegistry.register("InsightsRpc", this.insightsRpc);
   }

   private void enableInsightsComponentsIfNecessary(InsightsRuntimeConfig previousConfig, InsightsRuntimeConfig newConfig) {
      if(newConfig.isEnabled()) {
         logger.debug("Enabling the Insights Plugin Components");
         UnmodifiableIterator var3 = this.components.iterator();

         while(true) {
            while(var3.hasNext()) {
               InsightsRuntimeConfigComponent component = (InsightsRuntimeConfigComponent)var3.next();
               if(component.getNameForFiltering().isPresent()) {
                  FilteringRule rule = FilteringRule.applyFilters((String)component.getNameForFiltering().get(), newConfig.config.filtering_rules);
                  if(!rule.isAllowRule) {
                     if(component.isStarted()) {
                        component.stop();
                     }
                     continue;
                  }
               }

               if(component.shouldRestart(previousConfig, newConfig)) {
                  this.disableComponent(component);
               }

               this.enableComponent(component);
            }

            return;
         }
      }
   }

   public void onPreDeactivate() {
      this.disableInsightsComponents();
      this.runtimeConfigManager.stop();
   }

   private void disableInsightsComponents() {
      logger.debug("Deactivating Insights Plugin Components");
      UnmodifiableIterator var1 = this.components.iterator();

      while(var1.hasNext()) {
         InsightsRuntimeConfigComponent component = (InsightsRuntimeConfigComponent)var1.next();
         this.disableComponent(component);
      }

      this.insightsRpc.setFiltered();
   }

   private void enableComponent(InsightsRuntimeConfigComponent component) {
      this.disableOrEnableComponent(true, component);
   }

   private void disableComponent(InsightsRuntimeConfigComponent component) {
      this.disableOrEnableComponent(false, component);
   }

   private void disableOrEnableComponent(boolean enabled, InsightsRuntimeConfigComponent component) {
      try {
         if(enabled && !component.isStarted()) {
            component.start();
         } else if(!enabled && component.isStarted()) {
            component.stop();
         }
      } catch (Exception var4) {
         if(enabled) {
            logger.warn("Error enabling component", var4);
         } else {
            logger.warn("Error disabling component", var4);
         }
      }

   }

   public void onConfigChanged(InsightsRuntimeConfig previousConfig, InsightsRuntimeConfig newConfig) {
      if(newConfig.isEnabled()) {
         this.enableInsightsComponentsIfNecessary(previousConfig, newConfig);
      } else {
         this.disableInsightsComponents();
      }

      this.maybeEnableRpc(newConfig);
   }
}
