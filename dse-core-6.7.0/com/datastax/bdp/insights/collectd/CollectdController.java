package com.datastax.bdp.insights.collectd;

import com.datastax.bdp.config.InsightsConfig;
import com.datastax.bdp.insights.storage.config.InsightsRuntimeConfig;
import com.datastax.insights.client.TokenStore;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SigarLibrary;
import org.apache.commons.lang3.StringUtils;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectdController {
   public static final Supplier<CollectdController> instance = Suppliers.memoize(CollectdController::new);
   private static final MustacheFactory mf = new DefaultMustacheFactory();
   private static final Logger logger = LoggerFactory.getLogger(CollectdController.class);
   private static final String errorPreamble;
   public static final Integer MAX_SOCKET_NAME;
   public static final Supplier<String> defaultSocketFile;
   private CollectdController.CollectdConfig collectdConfig;
   private CollectdController.ScribeConfig scribeConfig;
   private CollectdController.ProcessState currentState;
   private int collectdPid;

   private CollectdController() {
      this.currentState = CollectdController.ProcessState.INIT;
      this.collectdConfig = null;
      this.scribeConfig = null;
      this.collectdPid = 0;
   }

   private synchronized int findCollectdPid() {
      assert this.collectdConfig != null;

      try {
         if(!Files.exists(Paths.get(this.collectdConfig.pidFile, new String[0]), new LinkOption[0])) {
            if(this.collectdPid > 0) {
               this.stop();
            }

            return -1;
         } else {
            List<String> pidFileLines = Files.readAllLines(Paths.get(this.collectdConfig.pidFile, new String[0]));
            if(pidFileLines.size() != 0 && !StringUtils.isEmpty((CharSequence)pidFileLines.get(0))) {
               int pid = Integer.valueOf((String)pidFileLines.get(0)).intValue();
               boolean pidIsCollectd;
               if(SigarLibrary.instance.initialized()) {
                  try {
                     pidIsCollectd = SigarLibrary.instance.sigar.getProcExe((long)pid).getName().contains("collectd");
                  } catch (SigarException var5) {
                     pidIsCollectd = false;
                  }
               } else {
                  pidIsCollectd = ((Boolean)ShellUtils.executeShellWithHandlers(String.format("/bin/ps -p %d -o command=", new Object[]{Integer.valueOf(pid)}), (input, err) -> {
                     String processName = input.readLine().toLowerCase();
                     return processName.contains("collectd")?Boolean.valueOf(true):Boolean.valueOf(false);
                  }, (exitCode, err) -> {
                     return Boolean.valueOf(false);
                  })).booleanValue();
               }

               return pidIsCollectd?pid:-1;
            } else {
               return -1;
            }
         }
      } catch (Exception var6) {
         if(this.currentState != CollectdController.ProcessState.INIT) {
            logger.warn("Collectd PID check error", var6);
         }

         return -1;
      }
   }

   public Optional<File> findCollectd() {
      String collectdRoot = (String)InsightsConfig.collectdRoot.get();
      if(!Files.isDirectory(Paths.get(collectdRoot, new String[0]), new LinkOption[0])) {
         logger.error("{}Collectd root directory wrong. Please fix in dse.yaml : {}", errorPreamble, Paths.get(collectdRoot, new String[0]).toAbsolutePath());
         return Optional.empty();
      } else {
         Path fullPath = Paths.get(collectdRoot, new String[]{"usr", "sbin", "collectd_wrapper"});
         if(!Files.exists(fullPath, new LinkOption[0])) {
            logger.error("{}Collectd root found but {} is missing", errorPreamble, fullPath.toAbsolutePath());
            return Optional.empty();
         } else if(!Files.isExecutable(fullPath)) {
            logger.error("{}{} is not executable", errorPreamble, fullPath.toAbsolutePath());
            return Optional.empty();
         } else {
            return Optional.of(fullPath.toFile());
         }
      }
   }

   private CollectdController.CollectdConfig generateCollectdConf(String socketFile) {
      this.collectdConfig = (new CollectdController.CollectdConfig.Builder()).withCollectdRoot((String)InsightsConfig.collectdRoot.get()).withSocketFile(socketFile).withLogDir((String)InsightsConfig.logDirectory.get()).build();
      return this.collectdConfig;
   }

   private CollectdController.ScribeConfig generateScribeConf(InsightsRuntimeConfig insightsConfig, TokenStore tokenStore) {
      assert insightsConfig != null;

      assert this.collectdConfig != null;

      try {
         CollectdController.ScribeConfig.Builder builder = (new CollectdController.ScribeConfig.Builder()).withInsightsUploadEnabled(insightsConfig.isInsightsUploadEnabled()).withInsightsStreamingEnabled(insightsConfig.isInsightsStreamingEnabled()).withWriteToDiskEnabled(insightsConfig.isWriteToDiskEnabled()).withDataDir((String)InsightsConfig.dataDirectory.get()).withServiceURL(new URL(insightsConfig.config.upload_url)).withUploadInterval(insightsConfig.config.upload_interval_in_seconds).withMaxDirSizeBytes(Long.valueOf((long)(insightsConfig.config.data_dir_max_size_in_mb.intValue() * 1000000))).withMetricUpdateGapInSeconds(Long.valueOf(insightsConfig.metricUpdateGapInSeconds())).withExistingConfigFile(this.collectdConfig.scribeConfigFile);
         Optional<String> token = tokenStore.token();
         if(token.isPresent()) {
            builder = builder.withToken((String)token.get());
         }

         this.scribeConfig = builder.build();
         return this.scribeConfig;
      } catch (MalformedURLException var5) {
         throw new RuntimeException(var5);
      }
   }

   public synchronized CollectdController.ProcessState start(String socketFile, InsightsRuntimeConfig insightsConfig, TokenStore tokenStore) {
      CollectdController.CollectdConfig collectdConfig = this.generateCollectdConf(socketFile);
      this.collectdPid = this.findCollectdPid();
      if(this.collectdPid > 0) {
         this.stop();

         assert this.currentState == CollectdController.ProcessState.STOPPED;
      }

      Optional<File> collectdExe = this.findCollectd();
      if(!collectdExe.isPresent()) {
         this.currentState = CollectdController.ProcessState.BROKEN;
         return this.currentState;
      } else {
         try {
            if(collectdConfig != null) {
               collectdConfig.generate();
            }

            CollectdController.ScribeConfig scribeConfig = this.generateScribeConf(insightsConfig, tokenStore);
            if(scribeConfig != null) {
               scribeConfig.generate();
            }

            boolean success = ((Boolean)ShellUtils.executeShellWithHandlers(((File)collectdExe.get()).getAbsolutePath() + " -C " + collectdConfig.configFile.getAbsolutePath() + " -P " + collectdConfig.pidFile, (input, output) -> {
               long start = System.nanoTime();

               while(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5L)) {
                  if(this.findCollectdPid() > 0) {
                     return Boolean.valueOf(true);
                  }

                  Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
               }

               return Boolean.valueOf(false);
            }, (exitCode, error) -> {
               String line;
               while((line = error.readLine()) != null) {
                  logger.warn(line);
               }

               logger.warn("Exit code = {}", exitCode);
               return Boolean.valueOf(false);
            }, ImmutableMap.of("BASEDIR", (new File(collectdConfig.collectdRoot)).getAbsolutePath(), "LD_PRELOAD", "", "DYLD_INSERT_LIBRARIES", ""))).booleanValue();
            if(success) {
               this.collectdPid = this.findCollectdPid();
               this.currentState = this.collectdPid > 0?CollectdController.ProcessState.STARTED:CollectdController.ProcessState.BROKEN;
            } else {
               logger.error("Collectd start failed");
               this.currentState = CollectdController.ProcessState.UNKNOWN;
            }
         } catch (IOException var8) {
            logger.error("Exception starting collectd", var8);
            this.currentState = CollectdController.ProcessState.UNKNOWN;
         }

         return this.currentState;
      }
   }

   public boolean healthCheck() {
      logger.debug("Current State = {}", this.currentState);
      return this.currentState == CollectdController.ProcessState.STARTED && this.collectdPid > 0 && this.collectdPid == this.findCollectdPid();
   }

   public synchronized CollectdController.ProcessState stop() {
      if(this.currentState != CollectdController.ProcessState.STOPPED && this.collectdPid >= 1) {
         try {
            logger.info("Stopping collectd");
            boolean killed = ((Boolean)ShellUtils.executeShellWithHandlers(String.format("kill %d", new Object[]{Integer.valueOf(this.collectdPid)}), (input, err) -> {
               return Boolean.valueOf(true);
            }, (exitcode, err) -> {
               return Boolean.valueOf(false);
            })).booleanValue();
            this.currentState = killed?CollectdController.ProcessState.STOPPED:CollectdController.ProcessState.UNKNOWN;
            this.collectdPid = -1;
         } catch (IOException var2) {
            logger.error("Error stopping collectd", var2);
            this.currentState = CollectdController.ProcessState.UNKNOWN;
         }

         return this.currentState;
      } else {
         return this.currentState;
      }
   }

   public synchronized CollectdController.ProcessState reloadPlugin(InsightsRuntimeConfig newConfig, TokenStore tokenStore) {
      try {
         logger.info("Generating new scribe config");
         this.scribeConfig = this.generateScribeConf(newConfig, tokenStore);
         this.scribeConfig.generate();
      } catch (IOException var4) {
         logger.error("Error reloading", var4);
         this.currentState = CollectdController.ProcessState.UNKNOWN;
      }

      if(this.currentState != CollectdController.ProcessState.INIT) {
         logger.debug("Insights reloaded {}", this.currentState);
      }

      return this.currentState;
   }

   static {
      errorPreamble = FBUtilities.isLinux?"":"Collectd is only supported on Linux, ";
      MAX_SOCKET_NAME = Integer.valueOf(103);
      defaultSocketFile = Suppliers.memoize(() -> {
         File f = null;

         try {
            f = File.createTempFile("dse-", ".sock");
         } catch (IOException var3) {
            throw new IOError(var3);
         }

         String socketFile = f.getAbsolutePath();
         if(socketFile.length() > MAX_SOCKET_NAME.intValue()) {
            String tmp = socketFile.substring(0, MAX_SOCKET_NAME.intValue());
            logger.warn("The unix socket ({}) path is greater than the unix standard limit, cropping to {}", socketFile, tmp);
            socketFile = tmp;
         }

         return socketFile;
      });
   }

   static class ScribeConfig {
      private static String SCRIBE_CONF_TEMPLATE = "scribe.conf.tmpl";
      private static final Mustache scribeConf;
      public final boolean insightsUploadEnabled;
      public final boolean insightsStreamingEnabled;
      public final boolean writeToDiskEnabled;
      public final Integer port;
      public final String host;
      public final String httpPath;
      public final Integer uploadIntervalSec;
      public final Long metricUpdateGapInSec;
      public final Long maxDataDirSizeBytes;
      public final String token;
      public final String dataDir;
      public final Boolean isSSL;
      public final File configFile;
      public final String caCertFile;

      private ScribeConfig(boolean insightsUploadEnabled, boolean insightsStreamingEnabled, boolean writeToDiskEnabled, Integer port, String host, String httpPath, Boolean isSSL, Integer uploadIntervalSec, Long metricUpdateGapInSec, Long maxDataDirSizeBytes, String token, String dataDir, File existingConfigFile) {
         this.caCertFile = InsightsConfig.DEFAULT_CACERT_FILE;
         this.insightsUploadEnabled = insightsUploadEnabled || insightsStreamingEnabled;
         this.insightsStreamingEnabled = insightsStreamingEnabled;
         this.writeToDiskEnabled = writeToDiskEnabled;
         this.port = port;
         this.host = host;
         this.httpPath = httpPath;
         this.isSSL = isSSL;
         this.uploadIntervalSec = uploadIntervalSec;
         this.metricUpdateGapInSec = metricUpdateGapInSec;
         this.maxDataDirSizeBytes = maxDataDirSizeBytes;
         this.dataDir = dataDir;
         this.token = token;
         this.configFile = existingConfigFile;
      }

      synchronized File generate() throws IOException {
         assert this.configFile != null;

         FileWriter writer = new FileWriter(this.configFile);
         Throwable var2 = null;

         try {
            scribeConf.execute(writer, this);
         } catch (Throwable var11) {
            var2 = var11;
            throw var11;
         } finally {
            if(writer != null) {
               if(var2 != null) {
                  try {
                     writer.close();
                  } catch (Throwable var10) {
                     var2.addSuppressed(var10);
                  }
               } else {
                  writer.close();
               }
            }

         }

         return this.configFile;
      }

      static {
         scribeConf = CollectdController.mf.compile(SCRIBE_CONF_TEMPLATE);
      }

      static class Builder {
         private URL serviceUrl;
         private Integer intervalSec;
         private Long maxDataDirSizeBytes;
         private Long metricUpdateGapInSec;
         private String dataDir;
         private String token;
         private File existingConfigFile;
         private boolean insightsUploadEnabled;
         private boolean insightsStreamingEnabled;
         private boolean writeToDiskEnabled;

         Builder() {
         }

         CollectdController.ScribeConfig.Builder withServiceURL(URL serviceUrl) {
            this.serviceUrl = serviceUrl;
            return this;
         }

         CollectdController.ScribeConfig.Builder withToken(String token) {
            this.token = token;
            return this;
         }

         CollectdController.ScribeConfig.Builder withDataDir(String dataDir) {
            Path path = Paths.get(dataDir, new String[0]);
            FileUtils.createDirectory(dataDir);
            if(!Files.isWritable(path)) {
               throw new RuntimeException("dataDir not writable " + dataDir);
            } else {
               this.dataDir = path.toFile().getAbsolutePath();
               return this;
            }
         }

         CollectdController.ScribeConfig.Builder withExistingConfigFile(File existingConfigFile) {
            this.existingConfigFile = existingConfigFile;
            return this;
         }

         CollectdController.ScribeConfig.Builder withUploadInterval(Integer intervalSec) {
            assert intervalSec.intValue() >= 0;

            this.intervalSec = intervalSec;
            return this;
         }

         CollectdController.ScribeConfig.Builder withMaxDirSizeBytes(Long sizeInBytes) {
            assert sizeInBytes.longValue() > 1048576L : "Size must be > 1mb";

            this.maxDataDirSizeBytes = sizeInBytes;
            return this;
         }

         CollectdController.ScribeConfig.Builder withMetricUpdateGapInSeconds(Long metricUpdateGapInSec) {
            assert metricUpdateGapInSec.longValue() >= 0L;

            this.metricUpdateGapInSec = metricUpdateGapInSec;
            return this;
         }

         CollectdController.ScribeConfig.Builder withInsightsUploadEnabled(boolean insightsUploadEnabled) {
            this.insightsUploadEnabled = insightsUploadEnabled;
            return this;
         }

         CollectdController.ScribeConfig.Builder withInsightsStreamingEnabled(boolean insightsStreamingEnabled) {
            this.insightsStreamingEnabled = insightsStreamingEnabled;
            return this;
         }

         CollectdController.ScribeConfig.Builder withWriteToDiskEnabled(boolean writeToDiskEnabled) {
            this.writeToDiskEnabled = writeToDiskEnabled;
            return this;
         }

         CollectdController.ScribeConfig build() {
            int port = this.serviceUrl.getPort() == -1?this.serviceUrl.getDefaultPort():this.serviceUrl.getPort();
            String host = this.serviceUrl.getHost();
            boolean isSSL = this.serviceUrl.getProtocol().equalsIgnoreCase("https");
            this.insightsUploadEnabled = this.insightsUploadEnabled || this.insightsStreamingEnabled;
            if(this.insightsUploadEnabled) {
               try {
                  InetAddress.getByName(host).getHostAddress();
               } catch (UnknownHostException var5) {
                  CollectdController.logger.warn("Insights service url {} is unreachable, no insights will be sent. This can be fixed with dsetool insights_config --upload_url", this.serviceUrl.toString());
                  host = null;
               }
            }

            return new CollectdController.ScribeConfig(this.insightsUploadEnabled, this.insightsStreamingEnabled, this.writeToDiskEnabled, Integer.valueOf(port), host, this.serviceUrl.getPath(), Boolean.valueOf(isSSL), this.intervalSec, this.metricUpdateGapInSec, this.maxDataDirSizeBytes, this.token, this.dataDir, this.existingConfigFile);
         }
      }
   }

   static class CollectdConfig {
      private static String COLLECTD_CONF_TEMPLATE = "collectd.conf.tmpl";
      private static final Mustache collectdConf;
      public final String collectdRoot;
      public final String logDir;
      public final String pidFile;
      public final String socketFile;
      public final Boolean isMac;
      public final String hostName;
      public final String dataCenter;
      public final String rack;
      public final String cluster;
      public final File configFile;
      public final File scribeConfigFile;

      private CollectdConfig(String collectdRoot, String logDir, String socketFile) {
         this.isMac = Boolean.valueOf(FBUtilities.isMacOSX);
         this.hostName = FBUtilities.getBroadcastAddress().getHostAddress();
         this.dataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
         this.rack = DatabaseDescriptor.getEndpointSnitch().getLocalRack();
         this.cluster = DatabaseDescriptor.getClusterName();
         this.collectdRoot = collectdRoot;
         this.logDir = logDir;
         this.pidFile = Paths.get(logDir, new String[]{"dse-collectd.pid"}).toFile().getAbsolutePath();
         this.socketFile = socketFile;

         try {
            this.configFile = Files.createTempFile("dse-collectd-", ".conf", new FileAttribute[]{PosixFilePermissions.asFileAttribute(ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE))}).toFile();
            this.scribeConfigFile = Files.createTempFile("dse-collectd-scribe-", ".conf", new FileAttribute[]{PosixFilePermissions.asFileAttribute(ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE))}).toFile();
            this.scribeConfigFile.deleteOnExit();
            this.configFile.deleteOnExit();
         } catch (IOException var5) {
            throw new RuntimeException(var5);
         }
      }

      synchronized File generate() throws IOException {
         FileWriter writer = new FileWriter(this.configFile);
         Throwable var2 = null;

         try {
            collectdConf.execute(writer, this);
         } catch (Throwable var11) {
            var2 = var11;
            throw var11;
         } finally {
            if(writer != null) {
               if(var2 != null) {
                  try {
                     writer.close();
                  } catch (Throwable var10) {
                     var2.addSuppressed(var10);
                  }
               } else {
                  writer.close();
               }
            }

         }

         return this.configFile;
      }

      static {
         collectdConf = CollectdController.mf.compile(COLLECTD_CONF_TEMPLATE);
      }

      static class Builder {
         private String collectdRoot;
         private String logDir;
         private String socketFile;

         Builder() {
         }

         CollectdController.CollectdConfig.Builder withCollectdRoot(String collectdRoot) {
            Path path = Paths.get(collectdRoot, new String[0]);
            if(!Files.isDirectory(path, new LinkOption[0])) {
               throw new RuntimeException("collectdRoot missing " + collectdRoot);
            } else {
               this.collectdRoot = path.toFile().getAbsolutePath();
               return this;
            }
         }

         CollectdController.CollectdConfig.Builder withLogDir(String logDir) {
            assert logDir != null;

            FileUtils.createDirectory(logDir);
            Path path = Paths.get(logDir, new String[0]);
            if(!Files.isWritable(path)) {
               throw new RuntimeException("logDir not writable " + logDir);
            } else {
               this.logDir = path.toFile().getAbsolutePath();
               return this;
            }
         }

         CollectdController.CollectdConfig.Builder withSocketFile(String socketFile) {
            assert socketFile != null;

            if(socketFile.length() >= CollectdController.MAX_SOCKET_NAME.intValue()) {
               String tmp = socketFile.substring(0, CollectdController.MAX_SOCKET_NAME.intValue());
               CollectdController.logger.warn("The unix socket ({}) path is greater than the unix standard limit, cropping to {}", socketFile, tmp);
               socketFile = tmp;
            }

            this.socketFile = socketFile;
            return this;
         }

         CollectdController.CollectdConfig build() {
            return new CollectdController.CollectdConfig(this.collectdRoot, this.logDir, this.socketFile);
         }
      }
   }

   public static enum ProcessState {
      INIT,
      UNKNOWN,
      STARTED,
      STOPPED,
      BROKEN;

      private ProcessState() {
      }
   }
}
