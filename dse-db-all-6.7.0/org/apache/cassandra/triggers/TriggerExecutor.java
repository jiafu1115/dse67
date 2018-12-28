package org.apache.cassandra.triggers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TriggerExecutor {
   public static final TriggerExecutor instance = new TriggerExecutor();
   private final Map<String, ITrigger> cachedTriggers = Maps.newConcurrentMap();
   private final ClassLoader parent = Thread.currentThread().getContextClassLoader();
   private volatile ClassLoader customClassLoader;

   private TriggerExecutor() {
      this.reloadClasses();
   }

   public void reloadClasses() {
      File triggerDirectory = FBUtilities.cassandraTriggerDir();
      if(triggerDirectory != null) {
         this.customClassLoader = new CustomClassLoader(this.parent, triggerDirectory);
         this.cachedTriggers.clear();
      }
   }

   public PartitionUpdate execute(PartitionUpdate updates) throws InvalidRequestException {
      List<Mutation> intermediate = this.executeInternal(updates);
      if(intermediate != null && !intermediate.isEmpty()) {
         List<PartitionUpdate> augmented = this.validateForSinglePartition(updates.metadata().id, updates.partitionKey(), intermediate);
         augmented.add(updates);
         return PartitionUpdate.merge(augmented);
      } else {
         return updates;
      }
   }

   public Collection<Mutation> execute(Collection<? extends IMutation> mutations) throws InvalidRequestException {
      boolean hasCounters = false;
      List<Mutation> augmentedMutations = null;
      Iterator var4 = mutations.iterator();

      while(var4.hasNext()) {
         IMutation mutation = (IMutation)var4.next();
         if(mutation instanceof CounterMutation) {
            hasCounters = true;
         }

         Iterator var6 = mutation.getPartitionUpdates().iterator();

         while(var6.hasNext()) {
            PartitionUpdate upd = (PartitionUpdate)var6.next();
            List<Mutation> augmentations = this.executeInternal(upd);
            if(augmentations != null && !augmentations.isEmpty()) {
               this.validate(augmentations);
               if(augmentedMutations == null) {
                  augmentedMutations = new LinkedList();
               }

               augmentedMutations.addAll(augmentations);
            }
         }
      }

      if(augmentedMutations == null) {
         return null;
      } else if(hasCounters) {
         throw new InvalidRequestException("Counter mutations and trigger mutations cannot be applied together atomically.");
      } else {
         return this.mergeMutations(Iterables.concat(mutations, augmentedMutations));
      }
   }

   private Collection<Mutation> mergeMutations(Iterable<Mutation> mutations) {
      ListMultimap<Pair<String, ByteBuffer>, Mutation> groupedMutations = ArrayListMultimap.create();
      Iterator var3 = mutations.iterator();

      Pair key;
      while(var3.hasNext()) {
         Mutation mutation = (Mutation)var3.next();
         key = Pair.create(mutation.getKeyspaceName(), mutation.key().getKey());
         groupedMutations.put(key, mutation);
      }

      List<Mutation> merged = new ArrayList(groupedMutations.size());
      Iterator var7 = groupedMutations.keySet().iterator();

      while(var7.hasNext()) {
         key = (Pair)var7.next();
         merged.add(Mutation.merge(groupedMutations.get(key)));
      }

      return merged;
   }

   private List<PartitionUpdate> validateForSinglePartition(TableId tableId, DecoratedKey key, Collection<Mutation> tmutations) throws InvalidRequestException {
      this.validate(tmutations);
      ArrayList updates;
      if(tmutations.size() == 1) {
         updates = Lists.newArrayList(((Mutation)Iterables.getOnlyElement(tmutations)).getPartitionUpdates());
         if(updates.size() > 1) {
            throw new InvalidRequestException("The updates generated by triggers are not all for the same partition");
         } else {
            this.validateSamePartition(tableId, key, (PartitionUpdate)Iterables.getOnlyElement(updates));
            return updates;
         }
      } else {
         updates = new ArrayList(tmutations.size());
         Iterator var5 = tmutations.iterator();

         while(var5.hasNext()) {
            Mutation mutation = (Mutation)var5.next();
            Iterator var7 = mutation.getPartitionUpdates().iterator();

            while(var7.hasNext()) {
               PartitionUpdate update = (PartitionUpdate)var7.next();
               this.validateSamePartition(tableId, key, update);
               updates.add(update);
            }
         }

         return updates;
      }
   }

   private void validateSamePartition(TableId tableId, DecoratedKey key, PartitionUpdate update) throws InvalidRequestException {
      if(!key.equals(update.partitionKey())) {
         throw new InvalidRequestException("Partition key of additional mutation does not match primary update key");
      } else if(!tableId.equals(update.metadata().id)) {
         throw new InvalidRequestException("table of additional mutation does not match primary update table");
      }
   }

   private void validate(Collection<Mutation> tmutations) throws InvalidRequestException {
      Iterator var2 = tmutations.iterator();

      while(var2.hasNext()) {
         Mutation mutation = (Mutation)var2.next();
         QueryProcessor.validateKey(mutation.key().getKey());
         Iterator var4 = mutation.getPartitionUpdates().iterator();

         while(var4.hasNext()) {
            PartitionUpdate update = (PartitionUpdate)var4.next();
            update.validate();
         }
      }

   }

   private List<Mutation> executeInternal(PartitionUpdate update) {
      Triggers triggers = update.metadata().triggers;
      if(triggers.isEmpty()) {
         return null;
      } else {
         List<Mutation> tmutations = Lists.newLinkedList();
         Thread.currentThread().setContextClassLoader(this.customClassLoader);

         LinkedList var15;
         try {
            Iterator var4 = triggers.iterator();

            while(var4.hasNext()) {
               TriggerMetadata td = (TriggerMetadata)var4.next();
               ITrigger trigger = (ITrigger)this.cachedTriggers.get(td.classOption);
               if(trigger == null) {
                  trigger = this.loadTriggerInstance(td.classOption);
                  this.cachedTriggers.put(td.classOption, trigger);
               }

               Collection<Mutation> temp = trigger.augmentNonBlocking(update);
               if(temp != null) {
                  tmutations.addAll(temp);
               }
            }

            var15 = tmutations;
         } catch (CassandraException var12) {
            throw var12;
         } catch (Exception var13) {
            throw new RuntimeException(String.format("Exception while executing trigger on table with ID: %s", new Object[]{update.metadata().id}), var13);
         } finally {
            Thread.currentThread().setContextClassLoader(this.parent);
         }

         return var15;
      }
   }

   public synchronized ITrigger loadTriggerInstance(String triggerName) throws Exception {
      if(this.cachedTriggers.get(triggerName) != null) {
         return (ITrigger)this.cachedTriggers.get(triggerName);
      } else if(this.customClassLoader == null) {
         throw new RuntimeException("Trigger directory doesn't exist.");
      } else {
         return (ITrigger)this.customClassLoader.loadClass(triggerName).getConstructor(new Class[0]).newInstance(new Object[0]);
      }
   }
}
