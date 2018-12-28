package com.datastax.bdp.insights.reporting;

import com.datastax.bdp.insights.InsightsRuntimeConfigComponent;
import com.datastax.bdp.insights.events.ExceptionInformation;
import com.datastax.bdp.util.DseUtil;
import com.datastax.insights.client.InsightsClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.utils.ErrorHandler;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.time.ApproximateTime;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ExceptionRuntimeManager implements InsightsRuntimeConfigComponent, ErrorHandler {
   private static final Logger logger = LoggerFactory.getLogger(ExceptionRuntimeManager.class);
   private final NonBlockingHashMap<Class, Long> lastException = new NonBlockingHashMap();
   private final InsightsClient client;
   private final AtomicBoolean started = new AtomicBoolean(false);

   @Inject
   public ExceptionRuntimeManager(InsightsClient client) {
      this.client = client;
   }

   public void start() {
      if(this.started.compareAndSet(false, true)) {
         JVMStabilityInspector.registerExceptionListener(this);
      }

   }

   public void stop() {
      if(this.started.compareAndSet(true, false)) {
         JVMStabilityInspector.unregisterExceptionListener(this);
      }

   }

   public boolean isStarted() {
      return this.started.get();
   }

   public Optional<String> getNameForFiltering() {
      return Optional.of("dse.insights.event.exception");
   }

   public void handleError(Throwable error) {
      Throwable root = DseUtil.getRootCause(error);
      Class rootClass = root.getClass();
      Long now = Long.valueOf(ApproximateTime.nanoTime());
      Long lastSeen = (Long)this.lastException.get(rootClass);
      if((lastSeen == null || now.longValue() - lastSeen.longValue() > TimeUnit.MILLISECONDS.toNanos(500L)) && this.lastException.putIfMatchAllowNull(rootClass, now, lastSeen) == lastSeen) {
         try {
            this.client.report(new ExceptionInformation(error));
         } catch (Exception var7) {
            logger.warn("Error reporting exception information to insights", var7);
         }
      }

   }
}
