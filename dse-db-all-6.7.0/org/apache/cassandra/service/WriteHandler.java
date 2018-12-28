package org.apache.cassandra.service;

import io.reactivex.Completable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TPCTimer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WriteHandler extends CompletableFuture<Void> implements MessageCallback<EmptyPayload> {
   protected static final Logger logger = LoggerFactory.getLogger(WriteHandler.class);
   private static final Response<EmptyPayload> LOCAL_RESPONSE;

   public WriteHandler() {
   }

   public abstract WriteEndpoints endpoints();

   public abstract ConsistencyLevel consistencyLevel();

   public abstract WriteType writeType();

   protected abstract long queryStartNanos();

   public abstract Void get() throws WriteTimeoutException, WriteFailureException;

   public abstract Completable toObservable();

   long currentTimeout() {
      long requestTimeout = this.writeType() == WriteType.COUNTER?DatabaseDescriptor.getCounterWriteRpcTimeout():DatabaseDescriptor.getWriteRpcTimeout();
      return TimeUnit.MILLISECONDS.toNanos(requestTimeout) - (ApolloTime.approximateNanoTime() - this.queryStartNanos());
   }

   public void onLocalResponse() {
      this.onResponse(LOCAL_RESPONSE);
   }

   public static WriteHandler create(WriteEndpoints endpoints, ConsistencyLevel consistencyLevel, WriteType writeType, long queryStartNanos, TPCTimer timer) {
      return builder(endpoints, consistencyLevel, writeType, queryStartNanos, timer).build();
   }

   public static WriteHandler.Builder builder(WriteEndpoints endpoints, ConsistencyLevel consistencyLevel, WriteType writeType, long queryStartNanos, TPCTimer timer) {
      return new WriteHandler.Builder(endpoints, consistencyLevel, writeType, queryStartNanos, timer);
   }

   static {
      LOCAL_RESPONSE = Response.local(Verbs.WRITES.WRITE, EmptyPayload.instance, -1L);
   }

   public static class Builder {
      private final WriteEndpoints endpoints;
      private final ConsistencyLevel consistencyLevel;
      private final WriteType writeType;
      private final long queryStartNanos;
      private final TPCTimer timer;
      private int blockFor;
      private ConsistencyLevel idealConsistencyLevel;
      private Object onResponseTasks;
      private Object onTimeoutTasks;
      private Object onFailureTasks;

      private Builder(WriteEndpoints endpoints, ConsistencyLevel consistencyLevel, WriteType writeType, long queryStartNanos, TPCTimer timer) {
         this.blockFor = -1;
         this.endpoints = endpoints;
         this.consistencyLevel = consistencyLevel;
         this.writeType = writeType;
         this.queryStartNanos = queryStartNanos;
         this.timer = timer;
      }

      public WriteHandler.Builder onResponse(Consumer<Response<EmptyPayload>> task) {
         this.onResponseTasks = setTaskOrAddToList(task, this.onResponseTasks);
         return this;
      }

      public WriteHandler.Builder onFailure(Consumer<FailureResponse<EmptyPayload>> task) {
         this.onFailureTasks = setTaskOrAddToList(task, this.onFailureTasks);
         return this;
      }

      public WriteHandler.Builder onTimeout(Consumer<InetAddress> task) {
         this.onTimeoutTasks = setTaskOrAddToList(task, this.onTimeoutTasks);
         return this;
      }

      public WriteHandler.Builder hintOnTimeout(Mutation mutation) {
         return this.consistencyLevel == ConsistencyLevel.ANY?this:this.onTimeout((host) -> {
            StorageProxy.maybeSubmitHint(mutation, (InetAddress)host, (WriteHandler)null);
         });
      }

      public WriteHandler.Builder hintOnFailure(Mutation mutation) {
         return this.onFailure((response) -> {
            StorageProxy.maybeSubmitHint(mutation, (InetAddress)response.from(), (WriteHandler)null);
         });
      }

      public WriteHandler.Builder blockFor(int blockFor) {
         this.blockFor = blockFor;
         return this;
      }

      WriteHandler.Builder withIdealConsistencyLevel(ConsistencyLevel idealConsistencyLevel) {
         this.idealConsistencyLevel = idealConsistencyLevel;
         return this;
      }

      private WriteHandler makeHandler() {
         return (WriteHandler)(this.consistencyLevel.isDatacenterLocal()?new WriteHandlers.DatacenterLocalHandler(this.endpoints, this.consistencyLevel, this.blockFor, this.writeType, this.queryStartNanos, this.timer):(this.consistencyLevel == ConsistencyLevel.EACH_QUORUM && this.endpoints.keyspace().getReplicationStrategy() instanceof NetworkTopologyStrategy?new WriteHandlers.DatacenterSyncHandler(this.endpoints, this.consistencyLevel, this.blockFor, this.writeType, this.queryStartNanos, this.timer):new WriteHandlers.SimpleHandler(this.endpoints, this.consistencyLevel, this.blockFor, this.writeType, this.queryStartNanos, this.timer)));
      }

      private static <T> List<T> freeze(List<T> l) {
         return l == null?UnmodifiableArrayList.emptyList():UnmodifiableArrayList.copyOf((Collection)l);
      }

      private WriteHandler withTasks(WriteHandler handler) {
         final Object onResponseTasks = freezeTaskOrList(this.onResponseTasks);
         final Object onTimeoutTasks = freezeTaskOrList(this.onTimeoutTasks);
         final Object onFailureTasks = freezeTaskOrList(this.onFailureTasks);
         return new WrappingWriteHandler(handler) {
            public void onResponse(Response<EmptyPayload> response) {
               super.onResponse(response);
               WriteHandler.Builder.acceptTaskOrListOfTasks(response, onResponseTasks, "onResponse");
            }

            public void onFailure(FailureResponse<EmptyPayload> response) {
               super.onFailure(response);
               WriteHandler.Builder.acceptTaskOrListOfTasks(response, onFailureTasks, "onFailure");
            }

            public void onTimeout(InetAddress host) {
               super.onTimeout(host);
               WriteHandler.Builder.acceptTaskOrListOfTasks(host, onTimeoutTasks, "onTimeout");
            }
         };
      }

      private static <T> void acceptTaskOrListOfTasks(T host, Object taskOrList, String taskType) {
         if(taskOrList instanceof List) {
            Iterator var3 = ((List)taskOrList).iterator();

            while(var3.hasNext()) {
               Consumer<T> task = (Consumer)var3.next();
               accept(task, host, taskType);
            }
         } else {
            accept((Consumer)taskOrList, host, taskType);
         }

      }

      private static Object freezeTaskOrList(Object taskOrList) {
         return !(taskOrList instanceof ArrayList) && taskOrList != null?taskOrList:freeze((List)taskOrList);
      }

      private static Object setTaskOrAddToList(Consumer task, Object taskOrList) {
         if(task instanceof ArrayList) {
            throw new IllegalArgumentException("tasks are not permitted to subclass ArrayList");
         } else {
            if(taskOrList == null) {
               taskOrList = task;
            } else if(taskOrList instanceof ArrayList) {
               ((ArrayList)taskOrList).add(task);
            } else {
               ArrayList<Consumer<InetAddress>> consumers = new ArrayList(2);
               consumers.add((Consumer)taskOrList);
               consumers.add(task);
               taskOrList = consumers;
            }

            return taskOrList;
         }
      }

      private WriteHandler withIdealConsistencyLevel(WriteHandler handler) {
         final WriteHandler delegateHandler = WriteHandler.create(this.endpoints, this.idealConsistencyLevel, this.writeType, this.queryStartNanos, this.timer);
         KeyspaceMetrics metrics = this.endpoints.keyspace().metric;
         delegateHandler.thenRun(() -> {
            metrics.idealCLWriteLatency.addNano(ApolloTime.approximateNanoTime() - this.queryStartNanos);
         }).exceptionally((e) -> {
            metrics.writeFailedIdealCL.inc();
            return null;
         });
         return new WrappingWriteHandler(handler) {
            private final AtomicInteger totalResponses;

            {
               this.totalResponses = new AtomicInteger(Builder.this.endpoints.liveCount());
            }

            private void countResponse() {
               if(this.totalResponses.decrementAndGet() == 0) {
                  delegateHandler.completeExceptionally(new RuntimeException("Got all responses for the delegate handler"));
               }

            }

            public void onResponse(Response<EmptyPayload> response) {
               super.onResponse(response);
               delegateHandler.onResponse(response);
               this.countResponse();
            }

            public void onFailure(FailureResponse<EmptyPayload> response) {
               super.onFailure(response);
               delegateHandler.onFailure(response);
               this.countResponse();
            }

            public void onTimeout(InetAddress host) {
               super.onTimeout(host);
               delegateHandler.onTimeout(host);
               this.countResponse();
            }
         };
      }

      private static <T> void accept(Consumer<T> task, T value, String taskType) {
         try {
            task.accept(value);
         } catch (Exception var4) {
            JVMStabilityInspector.inspectThrowable(var4);
            WriteHandler.logger.error("Unexpected error while executing post-write {} task with value {}", new Object[]{taskType, value, var4});
         }

      }

      public WriteHandler build() {
         WriteHandler handler = this.makeHandler();
         if(this.onResponseTasks != null || this.onFailureTasks != null || this.onTimeoutTasks != null) {
            handler = this.withTasks(handler);
         }

         if(this.idealConsistencyLevel != null) {
            handler = this.withIdealConsistencyLevel(handler);
         }

         return handler;
      }
   }
}
