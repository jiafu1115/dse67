package com.datastax.bdp.insights;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.bdp.insights.collectd.CollectdController;
import com.datastax.bdp.insights.storage.config.FilteringRule;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.datastax.bdp.insights.storage.credentials.DseInsightsTokenStore;
import com.datastax.bdp.insights.storage.schema.InsightsConfigChangeListener;
import com.datastax.insights.client.InsightsClient;
import com.datastax.insights.client.TokenStore;
import com.datastax.insights.client.internal.InsightJsonSerializer;
import com.datastax.insights.client.metrics.Metric;
import com.datastax.insights.client.metrics.RateStats;
import com.datastax.insights.client.metrics.SamplingStats;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.json.JacksonUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.UnixChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.time.ApproximateTime;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsightsUnixSocketClient implements InsightsClient, InsightsConfigChangeListener {
   private static final Logger logger = LoggerFactory.getLogger(InsightsUnixSocketClient.class);
   private static final InsightJsonSerializer jsonEventSerializer = new InsightJsonSerializer();
   private static final int BATCH_SIZE = 256;
   private static final String FILTER_INSIGHTS_TAG = "insight_filtered=true";
   private static final String INF_BUCKET = "bucket_inf";
   private static final long[] inputBuckets = (new EstimatedHistogram(90)).getBucketOffsets();
   private static final Pair<Long, String>[] latencyBuckets;
   private static final long[] latencyOffsets = new long[]{35L, 60L, 103L, 179L, 310L, 535L, 924L, 1597L, 2759L, 4768L, 8239L, 14237L, 24601L, 42510L, 73457L, 126934L, 219342L, 379022L, 654949L, 1131752L, 1955666L, 3379391L, 5839588L, 10090808L, 17436917L};
   private Long lastTokenRefreshNanos;
   private final AtomicBoolean started;
   private final String socketFile;
   private final MetricRegistry metricsRegistry;
   @VisibleForTesting
   final ConcurrentHashMap<String, Function<String, Integer>> metricProcessors;
   @VisibleForTesting
   final ConcurrentHashMap<String, Function<String, Integer>> globalFilteredMetricProcessors;
   @VisibleForTesting
   final ConcurrentHashMap<String, Function<String, Integer>> insightFilteredMetricProcessors;
   private final TimeUnit rateUnit;
   private final TimeUnit durationUnit;
   private final double rateFactor;
   private final double durationFactor;
   private final String ip;
   private final Map<String, String> globalTags;
   private Bootstrap bootstrap;
   private EventLoopGroup eventLoopGroup;
   volatile Channel channel;
   private InsightsRuntimeConfig runtimeConfig;
   private final TokenStore tokenStore;
   private ScheduledFuture metricReportFuture;
   private ScheduledFuture healthCheckFuture;
   private final AtomicLong successResponses;
   private final AtomicLong errorResponses;
   private final AtomicLong failedHealthChecks;
   private final AtomicLong reportingIntervalCount;

   public InsightsUnixSocketClient(MetricRegistry metricsRegistry, InsightsRuntimeConfig runtimeConfig, TokenStore tokenStore) {
      this((String)null, metricsRegistry, runtimeConfig, tokenStore, TimeUnit.SECONDS, TimeUnit.MICROSECONDS);
   }

   public InsightsUnixSocketClient(String socketFile, MetricRegistry metricsRegistry, InsightsRuntimeConfig runtimeConfig, TokenStore tokenStore) {
      this(socketFile, metricsRegistry, runtimeConfig, tokenStore, TimeUnit.SECONDS, TimeUnit.MICROSECONDS);
   }

   public InsightsUnixSocketClient(String socketFile, MetricRegistry metricsRegistry, InsightsRuntimeConfig runtimeConfig, TokenStore tokenStore, TimeUnit rateUnit, TimeUnit durationUnit) {
      this.started = new AtomicBoolean(false);
      this.socketFile = socketFile == null?(String)CollectdController.defaultSocketFile.get():socketFile;
      this.metricsRegistry = metricsRegistry;
      this.rateUnit = rateUnit;
      this.durationUnit = durationUnit;
      this.rateFactor = (double)rateUnit.toSeconds(1L);
      this.runtimeConfig = runtimeConfig;
      this.tokenStore = tokenStore;
      this.durationFactor = 1.0D / (double)durationUnit.toNanos(1L);
      this.ip = FBUtilities.getBroadcastAddress().getHostAddress();
      this.globalTags = ImmutableMap.of("host", this.ip, "cluster", DatabaseDescriptor.getClusterName(), "datacenter", DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter(), "rack", DatabaseDescriptor.getEndpointSnitch().getLocalRack());
      this.metricProcessors = new ConcurrentHashMap();
      this.globalFilteredMetricProcessors = new ConcurrentHashMap();
      this.insightFilteredMetricProcessors = new ConcurrentHashMap();
      this.metricReportFuture = null;
      this.healthCheckFuture = null;
      this.lastTokenRefreshNanos = Long.valueOf(0L);
      this.successResponses = new AtomicLong(0L);
      this.errorResponses = new AtomicLong(0L);
      this.failedHealthChecks = new AtomicLong(0L);
      this.reportingIntervalCount = new AtomicLong(0L);
   }

   private EventLoopGroup epollGroup() {
      assert Epoll.isAvailable();

      EventLoopGroup epollEventLoopGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("insights"));
      ((Bootstrap)((Bootstrap)this.bootstrap.group(epollEventLoopGroup)).channel(EpollDomainSocketChannel.class)).handler(this.createNettyPipeline());
      return epollEventLoopGroup;
   }

   private EventLoopGroup kqueueGroup() {
      assert KQueue.isAvailable();

      EventLoopGroup nioEventLoopGroup = new KQueueEventLoopGroup(1, new DefaultThreadFactory("insights"));
      ((Bootstrap)((Bootstrap)this.bootstrap.group(nioEventLoopGroup)).channel(KQueueDomainSocketChannel.class)).handler(this.createNettyPipeline());
      return nioEventLoopGroup;
   }

   private ChannelInitializer<UnixChannel> createNettyPipeline() {
      return new ChannelInitializer<UnixChannel>() {
         protected void initChannel(UnixChannel channel) throws Exception {
            channel.pipeline().addLast(new ChannelHandler[]{new LineBasedFrameDecoder(256)});
            channel.pipeline().addLast(new ChannelHandler[]{new StringDecoder(CharsetUtil.US_ASCII)});
            channel.pipeline().addLast(new ChannelHandler[]{new StringEncoder(CharsetUtil.US_ASCII)});
            channel.pipeline().addLast(new ChannelHandler[]{new SimpleChannelInboundHandler<String>() {
               protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
                  if(msg.startsWith("-1")) {
                     NoSpamLogger.getLogger(InsightsUnixSocketClient.logger, 5L, TimeUnit.SECONDS).warn("Collectd err: {}", new Object[]{msg});
                     System.out.println("Error: " + msg);
                     InsightsUnixSocketClient.this.errorResponses.incrementAndGet();
                  } else {
                     InsightsUnixSocketClient.logger.trace(msg);
                     InsightsUnixSocketClient.this.successResponses.incrementAndGet();
                  }

               }
            }});
         }
      };
   }

   public void start() {
      AtomicBoolean var1 = this.started;
      synchronized(this.started) {
         if(!this.started.get()) {
            this.bootstrap = new Bootstrap();
            if(SystemUtils.IS_OS_LINUX) {
               this.eventLoopGroup = this.epollGroup();
            } else {
               if(!SystemUtils.IS_OS_MAC_OSX) {
                  throw new RuntimeException("Unsupported OS");
               }

               this.eventLoopGroup = this.kqueueGroup();
            }

            try {
               CollectdController.ProcessState r = ((CollectdController)CollectdController.instance.get()).start(this.socketFile, this.runtimeConfig, this.tokenStore);
               if(r != CollectdController.ProcessState.STARTED) {
                  logger.warn("Not able to start collectd, will keep trying: {}", r);
               } else {
                  this.tryConnect();
               }
            } catch (Throwable var4) {
               logger.warn("Error connecting", var4);
            }

            boolean applied = this.started.compareAndSet(false, true);

            assert applied;

            this.initMetricsReporting();
            this.initCollectdHealthCheck();
            this.restartMetricReporting(this.runtimeConfig.config.metric_sampling_interval_in_seconds);
         } else {
            throw new RuntimeException("Insights Client is already started");
         }
      }
   }

   public void close() {
      AtomicBoolean var1 = this.started;
      synchronized(this.started) {
         if(this.started.compareAndSet(true, false)) {
            logger.info("Stopping Insights Client...");
            if(this.metricReportFuture != null) {
               this.metricReportFuture.cancel(true);
            }

            if(this.healthCheckFuture != null) {
               this.healthCheckFuture.cancel(true);
            }

            ((CollectdController)CollectdController.instance.get()).stop();
            if(this.channel != null) {
               this.channel.close().syncUninterruptibly();
               this.channel = null;
            }

            if(this.eventLoopGroup != null) {
               this.eventLoopGroup.shutdownGracefully();
               this.eventLoopGroup = null;
            }

         } else {
            throw new RuntimeException("Insights Client has already been stopped");
         }
      }
   }

   private void maybeGetToken() {
      if(this.runtimeConfig.isInsightsUploadEnabled()) {
         Optional<String> currentToken = this.tokenStore.token();
         Long now = Long.valueOf(ApproximateTime.nanoTime());
         if(now.longValue() - this.lastTokenRefreshNanos.longValue() > TimeUnit.SECONDS.toNanos(Math.max(TimeUnit.HOURS.toSeconds(1L), (long)this.runtimeConfig.config.upload_interval_in_seconds.intValue()))) {
            try {
               URL url = new URL(this.runtimeConfig.config.upload_url);
               URL tokenUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), "/api/v1/tokens");
               HttpURLConnection connection = (HttpURLConnection)tokenUrl.openConnection(this.runtimeConfig.proxyFromConfig());
               connection.setRequestMethod("POST");
               connection.setDoOutput(true);
               connection.setFixedLengthStreamingMode(0);
               if(currentToken.isPresent()) {
                  connection.setRequestProperty("Authorization", "Bearer " + (String)currentToken.get());
               }

               this.getAndStoreToken(connection);
            } catch (Throwable var9) {
               logger.warn("Error trying to refresh insights token", var9);
            } finally {
               this.lastTokenRefreshNanos = now;
            }
         }

      }
   }

   private void getAndStoreToken(HttpURLConnection connection) throws IOException {
      try {
         connection.connect();
         int code = connection.getResponseCode();
         if(code != 200 && code != 201) {
            logger.warn("Error trying to get insights token at {} {}", connection.getURL(), Integer.valueOf(code));
         } else {
            InputStreamReader stream = new InputStreamReader(connection.getInputStream());
            Throwable var4 = null;

            try {
               String token = CharStreams.toString(stream);
               this.tokenStore.store(token);
            } catch (Throwable var20) {
               var4 = var20;
               throw var20;
            } finally {
               if(stream != null) {
                  if(var4 != null) {
                     try {
                        stream.close();
                     } catch (Throwable var19) {
                        var4.addSuppressed(var19);
                     }
                  } else {
                     stream.close();
                  }
               }

            }

            if(((CollectdController)CollectdController.instance.get()).reloadPlugin(this.runtimeConfig, this.tokenStore) == CollectdController.ProcessState.STARTED) {
               this.reportInternalWithFlush("RELOADINSIGHTS", "");
            }
         }
      } finally {
         connection.disconnect();
      }

   }

   private void initCollectdHealthCheck() {
      this.healthCheckFuture = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> {
         try {
            if(!((CollectdController)CollectdController.instance.get()).healthCheck()) {
               this.failedHealthChecks.incrementAndGet();
               ((CollectdController)CollectdController.instance.get()).start(this.socketFile, this.runtimeConfig, this.tokenStore);
            }

            this.tryConnect();
            this.maybeGetToken();
            if(this.tokenStore instanceof DseInsightsTokenStore) {
               ((DseInsightsTokenStore)this.tokenStore).checkFingerprint(this);
            }
         } catch (Throwable var2) {
            logger.error("Error with collectd healthcheck", var2);
         }

      }, 30L, 30L, TimeUnit.SECONDS);
   }

   private void tryConnect() {
      if(this.channel == null || !this.channel.isOpen()) {
         try {
            this.channel = this.bootstrap.connect(new DomainSocketAddress(this.socketFile)).syncUninterruptibly().channel();
            this.errorResponses.set(0L);
            this.successResponses.set(0L);
            logger.info("Connection to collectd established");
         } catch (Throwable var2) {
            if(!(var2 instanceof IOException)) {
               throw var2;
            }

            logger.warn("Error connecting to collectd");
         }

      }
   }

   private synchronized void refreshFilters() {
      Set<String> seen = new HashSet();
      Iterator entries = Iterators.concat(this.metricProcessors.entrySet().iterator(), this.insightFilteredMetricProcessors.entrySet().iterator(), this.globalFilteredMetricProcessors.entrySet().iterator());

      while(entries.hasNext()) {
         Entry<String, Function<String, Integer>> e = (Entry)entries.next();
         if(seen.add(e.getKey())) {
            entries.remove();
            this.addMetric((String)e.getKey(), (Function)e.getValue());
         }
      }

      logger.info("unfiltered metrics {}, insight filtered metrics {}, globally filtered metrics {}", new Object[]{Integer.valueOf(this.metricProcessors.size()), Integer.valueOf(this.insightFilteredMetricProcessors.size()), Integer.valueOf(this.globalFilteredMetricProcessors.size())});
   }

   String clean(String name) {
      if(name.startsWith("jvm")) {
         name = name.replaceAll("\\-", "_");
         return name.toLowerCase();
      } else {
         name = name.replaceAll("\\s*,\\s*", ",");
         name = name.replaceAll("\\s+", "_");
         name = name.replaceAll("\\\\", "_");
         name = name.replaceAll("/", "_");
         name = name.replaceAll("[^a-zA-Z0-9\\.\\_]+", ".");
         name = name.replaceAll("\\.+", ".");
         name = name.replaceAll("_+", "_");
         name = String.join("_", name.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])"));
         name = name.replaceAll("\\._", "\\.");
         name = name.replaceAll("_+", "_");
         return name.toLowerCase();
      }
   }

   void addMetric(String name, Function<String, Integer> writer) {
      FilteringRule applied = FilteringRule.applyFilters(name, this.runtimeConfig.config.filtering_rules);
      FilteringRule applied2 = FilteringRule.applyFilters(this.clean(name), this.runtimeConfig.config.filtering_rules);
      if(applied.isAllowRule != applied2.isAllowRule) {
         applied = applied.isAllowRule?applied2:applied;
      } else if(applied.isGlobal != applied2.isGlobal) {
         applied = applied.isGlobal?applied:applied2;
      }

      logger.trace("Using filtering rule {} for name '{}'", applied, name);
      ConcurrentHashMap<String, Function<String, Integer>> picked = null;
      if(applied.isAllowRule) {
         picked = applied.isGlobal?this.metricProcessors:this.insightFilteredMetricProcessors;
      } else {
         picked = applied.isGlobal?this.globalFilteredMetricProcessors:this.insightFilteredMetricProcessors;
      }

      picked.put(name, writer);
   }

   private void initMetricsReporting() {
      if(this.metricsRegistry != null) {
         this.metricsRegistry.addListener(new MetricRegistryListener() {
            void removeMetric(String name) {
               Function<String, Integer> f = (Function)InsightsUnixSocketClient.this.metricProcessors.remove(name);
               if(f != null && !StorageService.instance.isShutdown()) {
                  f.apply("");
               }

               f = (Function)InsightsUnixSocketClient.this.insightFilteredMetricProcessors.remove(name);
               if(f != null && !StorageService.instance.isShutdown()) {
                  f.apply("insight_filtered=true");
               }

               InsightsUnixSocketClient.this.globalFilteredMetricProcessors.remove(name);
            }

            public void onGaugeAdded(String name, Gauge<?> gauge) {
               String cleanName = InsightsUnixSocketClient.this.clean(name);
               InsightsUnixSocketClient.this.addMetric(name, (tags) -> {
                  return Integer.valueOf(InsightsUnixSocketClient.this.writeMetric(cleanName, tags, gauge));
               });
            }

            public void onGaugeRemoved(String name) {
               this.removeMetric(name);
            }

            public void onCounterAdded(String name, Counter counter) {
               String cleanName = InsightsUnixSocketClient.this.clean(name);
               InsightsUnixSocketClient.this.addMetric(name, (tags) -> {
                  return Integer.valueOf(InsightsUnixSocketClient.this.writeMetric(cleanName, tags, counter));
               });
            }

            public void onCounterRemoved(String name) {
               this.removeMetric(name);
            }

            public void onHistogramAdded(String name, Histogram histogram) {
               String cleanName = InsightsUnixSocketClient.this.clean(name);
               InsightsUnixSocketClient.this.addMetric(name, (tags) -> {
                  return Integer.valueOf(InsightsUnixSocketClient.this.writeMetric(cleanName, tags, histogram));
               });
            }

            public void onHistogramRemoved(String name) {
               this.removeMetric(name);
            }

            public void onMeterAdded(String name, Meter meter) {
               String cleanName = InsightsUnixSocketClient.this.clean(name);
               InsightsUnixSocketClient.this.addMetric(name, (tags) -> {
                  return Integer.valueOf(InsightsUnixSocketClient.this.writeMetric(cleanName, tags, meter));
               });
            }

            public void onMeterRemoved(String name) {
               this.removeMetric(name);
            }

            public void onTimerAdded(String name, Timer timer) {
               String cleanName = InsightsUnixSocketClient.this.clean(name);
               InsightsUnixSocketClient.this.addMetric(name, (tags) -> {
                  return Integer.valueOf(InsightsUnixSocketClient.this.writeMetric(cleanName, tags, timer));
               });
            }

            public void onTimerRemoved(String name) {
               this.removeMetric(name);
            }
         });
      }
   }

   private synchronized void restartMetricReporting(Integer metricSamplingIntervalInSeconds) {
      if(this.metricReportFuture != null) {
         this.metricReportFuture.cancel(false);
      }

      logger.info("Starting metric reporting with {} sec interval", metricSamplingIntervalInSeconds);
      long reportInsightEvery = Math.max((long)Math.floor((double)(this.runtimeConfig.metricUpdateGapInSeconds() / (long)metricSamplingIntervalInSeconds.intValue())), 1L);
      logger.debug("Reporting metric insights every {} intervals", Long.valueOf(reportInsightEvery));
      this.metricReportFuture = this.eventLoopGroup.scheduleWithFixedDelay(() -> {
         if(this.channel == null || !this.channel.isOpen()) {
            logger.info("Metric reporting skipped due to connection to collectd not being established");
         }

         long count = 0L;
         long thisInterval = this.reportingIntervalCount.getAndIncrement();
         count += (long)this.writeGroup(this.metricProcessors, thisInterval % reportInsightEvery == 0L?"":"insight_filtered=true");
         count += (long)this.writeGroup(this.insightFilteredMetricProcessors, "insight_filtered=true");
         if(count > 0L) {
            logger.trace("Calling flush with {}", Long.valueOf(count));
            this.flush();
         }

      }, (long)metricSamplingIntervalInSeconds.intValue(), (long)metricSamplingIntervalInSeconds.intValue(), TimeUnit.SECONDS);
   }

   private int writeGroup(ConcurrentHashMap<String, Function<String, Integer>> group, String tags) {
      int count = 0;
      Iterator var4 = group.entrySet().iterator();

      while(var4.hasNext()) {
         Entry m = (Entry)var4.next();

         try {
            count += ((Integer)((Function)m.getValue()).apply(tags)).intValue();
            if(count >= 256) {
               logger.trace("Calling flush with {}", Integer.valueOf(count));
               this.flush();
               count = 0;
            }
         } catch (Throwable var7) {
            logger.warn("Error reporting: ", var7);
         }
      }

      return count;
   }

   private int writeMetric(String name, String tags, Gauge gauge) {
      Object value = gauge.getValue();
      if(!(value instanceof Number)) {
         logger.trace("Value not a number {} {}", name, value);
         return 0;
      } else {
         this.reportCollectd(name, tags, (Number)value, "gauge");
         return 1;
      }
   }

   private int writeMetric(String name, String tags, Counter counter) {
      this.reportCollectd(name, tags, Long.valueOf(counter.getCount()), "counter");
      return 1;
   }

   private int writeMetric(String name, String tags, Timer timer) {
      Snapshot snapshot = timer.getSnapshot();
      double meanRate = this.convertRate(timer.getMeanRate());
      double min1Rate = this.convertRate(timer.getOneMinuteRate());
      double min5rate = this.convertRate(timer.getFiveMinuteRate());
      double min15rate = this.convertRate(timer.getFifteenMinuteRate());
      double max = this.convertDuration((double)snapshot.getMax());
      double mean = this.convertDuration(snapshot.getMean());
      double min = this.convertDuration((double)snapshot.getMin());
      double stddev = this.convertDuration(snapshot.getStdDev());
      double p50 = this.convertDuration(snapshot.getMedian());
      double p75 = this.convertDuration(snapshot.get75thPercentile());
      double p90 = this.convertDuration(snapshot.getValue(0.9D));
      double p95 = this.convertDuration(snapshot.get95thPercentile());
      double p98 = this.convertDuration(snapshot.get98thPercentile());
      double p99 = this.convertDuration(snapshot.get99thPercentile());
      double p999 = this.convertDuration(snapshot.get999thPercentile());
      long count = timer.getCount();
      this.reportCollectdHistogram(name, "insight_filtered=true", count, max, mean, min, stddev, p50, p75, p90, p95, p98, p99, p999);
      this.reportCollectdMeter(name, "insight_filtered=true", count, meanRate, min1Rate, min5rate, min15rate);
      Map<String, String> buckets = this.reportPrometheusTimer(name, "insight_filtered=true", count, snapshot);
      int sent = 3;
      if(!tags.contains("insight_filtered=true")) {
         com.datastax.insights.client.metrics.Timer t = new com.datastax.insights.client.metrics.Timer(name, Long.valueOf(ApproximateTime.millisTime()), Optional.of(buckets), timer.getCount(), new SamplingStats((long)min, (long)max, mean, p50, p75, p95, p98, p99, p999, stddev), new RateStats(min1Rate, min5rate, min15rate, meanRate), this.rateUnit.name(), this.durationUnit.name());

         try {
            this.report((Metric)t);
         } catch (Exception var41) {
            throw new RuntimeException(var41);
         }

         ++sent;
      }

      return sent;
   }

   private int writeMetric(String name, String tags, Meter meter) {
      double meanRate = this.convertRate(meter.getMeanRate());
      double min1Rate = this.convertRate(meter.getOneMinuteRate());
      double min5rate = this.convertRate(meter.getFiveMinuteRate());
      double min15rate = this.convertRate(meter.getFifteenMinuteRate());
      this.reportCollectdMeter(name, "insight_filtered=true", meter.getCount(), meanRate, min1Rate, min5rate, min15rate);
      int sent = 1;
      if(!tags.contains("insight_filtered=true")) {
         com.datastax.insights.client.metrics.Meter m = new com.datastax.insights.client.metrics.Meter(name, Long.valueOf(ApproximateTime.millisTime()), Optional.of(this.globalTags), meter.getCount(), new RateStats(min1Rate, min5rate, min15rate, meanRate), this.rateUnit.name());

         try {
            this.report((Metric)m);
         } catch (Exception var15) {
            throw new RuntimeException(var15);
         }

         ++sent;
      }

      return sent;
   }

   private int writeMetric(String name, String tags, Histogram histogram) {
      Snapshot snapshot = histogram.getSnapshot();
      double max = this.convertDuration((double)snapshot.getMax());
      double mean = this.convertDuration(snapshot.getMean());
      double min = this.convertDuration((double)snapshot.getMin());
      double stddev = this.convertDuration(snapshot.getStdDev());
      double p50 = this.convertDuration(snapshot.getMedian());
      double p75 = this.convertDuration(snapshot.get75thPercentile());
      double p90 = this.convertDuration(snapshot.getValue(0.9D));
      double p95 = this.convertDuration(snapshot.get95thPercentile());
      double p98 = this.convertDuration(snapshot.get98thPercentile());
      double p99 = this.convertDuration(snapshot.get99thPercentile());
      double p999 = this.convertDuration(snapshot.get999thPercentile());
      this.reportCollectdHistogram(name, "insight_filtered=true", histogram.getCount(), max, mean, min, stddev, p50, p75, p90, p95, p98, p99, p999);
      int sent = 1;
      if(!tags.contains("insight_filtered=true")) {
         com.datastax.insights.client.metrics.Histogram h = new com.datastax.insights.client.metrics.Histogram(name, Long.valueOf(ApproximateTime.millisTime()), Optional.of(this.globalTags), histogram.getCount(), new SamplingStats((long)min, (long)max, mean, p50, p75, p95, p98, p99, p999, stddev));

         try {
            this.report((Metric)h);
         } catch (Exception var30) {
            throw new RuntimeException(var30);
         }

         ++sent;
      }

      return sent;
   }

   double convertDuration(double duration) {
      return duration * this.durationFactor;
   }

   double convertRate(double rate) {
      return rate * this.rateFactor;
   }

   public long getSuccessResponses() {
      return this.successResponses.get();
   }

   public long getErrorResponses() {
      return this.errorResponses.get();
   }

   public long getFailedHealthChecks() {
      return this.failedHealthChecks.get();
   }

   public void reconfigureHttpClient(boolean enableCompression, Proxy proxy, ProxySelector proxySelector, String proxyCustomAuthentication) {
   }

   public void reconfigureDataDirMaxSizeInMb(Integer dataDirMaxSizeInMb) {
   }

   public void reconfigureDataFileMaxSizeInMb(Integer dataFileMaxSizeInMb) {
   }

   public void reconfigureRolloverIntervalInSeconds(Integer dataFileRolloverIntervalInSeconds) {
   }

   public void reconfigureCollectorUrl(String newCollectorUrl) {
   }

   public void reconfigureCollectorUploadIntervalInSeconds(Integer collectorUploadIntervalInSeconds) {
   }

   boolean reportCollectdHistogram(String name, String tags, long count, double max, double mean, double min, double stddev, double p50, double p75, double p90, double p95, double p98, double p99, double p999) {
      int n = this.runtimeConfig.config.metric_sampling_interval_in_seconds.intValue();
      String msg = (new StringBuilder(256)).append(this.ip).append("/dse-").append(name).append("/").append("histogram interval=").append(n).append(" ").append(tags).append(" N:").append(count).append(":").append(max).append(":").append(mean).append(":").append(min).append(":").append(stddev).append(":").append(p50).append(":").append(p75).append(":").append(p90).append(":").append(p95).append(":").append(p98).append(":").append(p99).append(":").append(p999).toString();
      return this.reportInternalWithoutFlush("PUTVAL", msg);
   }

   boolean reportCollectdMeter(String name, String tags, long count, double meanRate, double min1Rate, double min5Rate, double min15Rate) {
      int n = this.runtimeConfig.config.metric_sampling_interval_in_seconds.intValue();
      String msg = (new StringBuilder(256)).append(this.ip).append("/dse-").append(name).append("/").append("meter interval=").append(n).append(" ").append(tags).append(" N:").append(count).append(":").append(meanRate).append(":").append(min1Rate).append(":").append(min5Rate).append(":").append(min15Rate).toString();
      return this.reportInternalWithoutFlush("PUTVAL", msg);
   }

   Map<String, String> reportPrometheusTimer(String name, String tags, long count, Snapshot snapshot) {
      int n = this.runtimeConfig.config.metric_sampling_interval_in_seconds.intValue();
      StringBuilder msg = (new StringBuilder(512)).append(this.ip).append("/dse-").append(name).append("/").append("micros interval=").append(n).append(" ").append(tags).append(" N:").append(count).append(":").append(snapshot.getMean() * (double)count);
      Map<String, String> bucketTags = Maps.newHashMapWithExpectedSize(this.globalTags.size() + latencyBuckets.length);
      bucketTags.putAll(this.globalTags);
      long[] buckets = inputBuckets;
      long[] values = snapshot.getValues();
      if(snapshot instanceof org.apache.cassandra.metrics.DecayingEstimatedHistogram.Snapshot) {
         buckets = ((org.apache.cassandra.metrics.DecayingEstimatedHistogram.Snapshot)snapshot).getOffsets();
      }

      if(values.length != buckets.length) {
         NoSpamLogger.getLogger(logger, 1L, TimeUnit.HOURS).debug("Not able to get buckets for {} {} type {}", new Object[]{name, Integer.valueOf(values.length), snapshot});
         return bucketTags;
      } else {
         int outputIndex = 0;
         long cumulativeCount = 0L;

         for(int i = 0; i < values.length; ++i) {
            if(outputIndex < latencyBuckets.length && buckets[i] > ((Long)latencyBuckets[outputIndex].left).longValue()) {
               String value = Long.toString(cumulativeCount);
               msg.append(":").append(value);
               bucketTags.put(latencyBuckets[outputIndex++].right, value);
            }

            cumulativeCount += values[i];
         }

         String total = Long.toString(cumulativeCount);

         while(outputIndex++ <= latencyBuckets.length) {
            msg.append(":").append(total);
            bucketTags.put("bucket_inf", total);
         }

         this.reportInternalWithoutFlush("PUTVAL", msg.toString());
         return bucketTags;
      }
   }

   boolean reportCollectd(String name, String tags, Number value, String type, String typeInstance) {
      int n = this.runtimeConfig.config.metric_sampling_interval_in_seconds.intValue();
      String msg = (new StringBuilder(256)).append(this.ip).append("/dse-").append(name).append("/").append(type).append("-").append(typeInstance).append(" ").append("interval=").append(n).append(" ").append(tags).append(" N:").append(value).toString();
      return this.reportInternalWithoutFlush("PUTVAL", msg);
   }

   boolean reportCollectd(String name, String tags, Number value, String type) {
      int n = this.runtimeConfig.config.metric_sampling_interval_in_seconds.intValue();
      String msg = (new StringBuilder(256)).append(this.ip).append("/dse-").append(name).append("/").append(type).append(" ").append("interval=").append(n).append(" ").append(tags).append(" N:").append(value).toString();
      return this.reportInternalWithoutFlush("PUTVAL", msg);
   }

   @VisibleForTesting
   boolean reportInternalWithFlush(String collectdAction, String insightJsonString) {
      return this.reportInternal(collectdAction, insightJsonString, true);
   }

   @VisibleForTesting
   boolean reportInternalWithoutFlush(String collectdAction, String insightJsonString) {
      return this.reportInternal(collectdAction, insightJsonString, false);
   }

   boolean reportInternal(String collectdAction, String insightJsonString, boolean flush) {
      if(!this.started.get()) {
         logger.warn("Insights Client is not started");
         return false;
      } else if(this.channel != null && this.channel.isOpen()) {
         try {
            this.channel.write(collectdAction + " " + insightJsonString + "\n");
            if(flush) {
               this.channel.flush();
            }

            return true;
         } catch (Throwable var5) {
            if(var5 instanceof IOException) {
               if(this.channel != null) {
                  this.channel.close().syncUninterruptibly();
               }

               this.channel = null;
            } else if(var5 instanceof RejectedExecutionException) {
               if(this.eventLoopGroup.isShutdown()) {
                  NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS).error("Insights reporter eventloop shutdown", new Object[0]);
               } else {
                  NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS).debug("Insight write queue full, dropping report", new Object[0]);
               }
            } else {
               logger.warn("Exception encountered reporting insight", var5);
            }

            return false;
         }
      } else {
         NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS).warn("Connection to Collectd not established", new Object[0]);
         return false;
      }
   }

   void flush() {
      if(this.channel != null && this.channel.isOpen()) {
         try {
            this.channel.flush();
         } catch (Throwable var2) {
            if(!(var2 instanceof IOException)) {
               throw var2;
            }

            if(this.channel != null) {
               this.channel.close().syncUninterruptibly();
            }

            this.channel = null;
         }

      } else {
         NoSpamLogger.getLogger(logger, 30L, TimeUnit.SECONDS).warn("Connection to Collectd not established", new Object[0]);
      }
   }

   public boolean report(String insightJsonString, InsightMetadata metadata) throws Exception {
      ObjectNode node = (ObjectNode)JacksonUtil.getObjectMapper().readValue(insightJsonString, ObjectNode.class);
      Insight insight = new Insight(metadata, node);
      return this.reportInternalWithFlush("PUTINSIGHT", jsonEventSerializer.serialize(insight));
   }

   public boolean report(Object insightData, InsightMetadata metadata) throws Exception {
      return this.reportInternalWithFlush("PUTINSIGHT", jsonEventSerializer.serialize(insightData, metadata));
   }

   public boolean report(Metric metricInsight) throws Exception {
      return this.reportInternalWithFlush("PUTINSIGHT", jsonEventSerializer.serialize(metricInsight));
   }

   public boolean report(Insight insight) throws Exception {
      return this.reportInternalWithFlush("PUTINSIGHT", jsonEventSerializer.serialize(insight));
   }

   public void onConfigChanged(InsightsRuntimeConfig previousConfig, InsightsRuntimeConfig newConfig) {
      this.runtimeConfig = newConfig;
      AtomicBoolean var3 = this.started;
      synchronized(this.started) {
         if(this.started.get()) {
            try {
               if(((CollectdController)CollectdController.instance.get()).reloadPlugin(newConfig, this.tokenStore) == CollectdController.ProcessState.STARTED) {
                  this.reportInternalWithFlush("RELOADINSIGHTS", "");
               }
            } catch (Exception var6) {
               logger.warn("Problem reloading insights config for collectd", var6);
            }

            if(!Objects.equals(previousConfig.config.metric_sampling_interval_in_seconds, newConfig.config.metric_sampling_interval_in_seconds) || !Objects.equals(previousConfig.config.upload_interval_in_seconds, newConfig.config.upload_interval_in_seconds)) {
               this.restartMetricReporting(newConfig.config.metric_sampling_interval_in_seconds);
            }

            if(!previousConfig.config.upload_url.equalsIgnoreCase(newConfig.config.upload_url)) {
               this.lastTokenRefreshNanos = Long.valueOf(0L);
            }

            if(!Objects.equals(previousConfig.config.filtering_rules, newConfig.config.filtering_rules)) {
               this.refreshFilters();
            }

         }
      }
   }

   static {
      latencyBuckets = new Pair[latencyOffsets.length];

      for(int i = 0; i < latencyBuckets.length; ++i) {
         latencyBuckets[i] = Pair.create(Long.valueOf(latencyOffsets[i] * 1000L), "bucket_" + Long.toString(latencyOffsets[i]));
      }

   }
}
