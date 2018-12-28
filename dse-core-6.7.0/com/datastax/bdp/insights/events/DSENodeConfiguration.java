package com.datastax.bdp.insights.events;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.datastax.insights.core.json.JacksonUtil;
import com.datastax.insights.core.json.JacksonUtil.JacksonUtilException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOError;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FBUtilities.CpuInfo;
import org.apache.cassandra.utils.time.ApproximateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DSENodeConfiguration extends Insight {
   private static final Logger logger = LoggerFactory.getLogger(DSENodeConfiguration.class);
   private static final String NAME = "dse.insights.event.node_configuration";
   private static final String MAPPING_VERSION = "dse-config-" + ProductVersion.getDSEVersionString();
   private static final ObjectWriter SECURE_WRITER = (new ObjectMapper()).addMixIn(Object.class, DSENodeConfiguration.SecureFilterMixIn.class).writer((new SimpleFilterProvider()).addFilter("secure filter", new DSENodeConfiguration.SecurePropertyFilter()));

   @JsonCreator
   public DSENodeConfiguration(@JsonProperty("metadata") InsightMetadata metadata, @JsonProperty("data") DSENodeConfiguration.Data data) {
      super(metadata, data);
   }

   public DSENodeConfiguration(DSENodeConfiguration.Data data) {
      super(new InsightMetadata("dse.insights.event.node_configuration", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.of(MAPPING_VERSION)), data);
   }

   public DSENodeConfiguration() {
      this(new DSENodeConfiguration.Data());
   }

   public DSENodeConfiguration.Data getData() {
      return (DSENodeConfiguration.Data)this.insightData;
   }

   static class SecurePropertyFilter extends SimpleBeanPropertyFilter {
      private static final Pattern pattern = Pattern.compile("(secret|user|pass)", 2);

      SecurePropertyFilter() {
      }

      private String createPath(PropertyWriter writer, JsonGenerator jgen) {
         StringBuilder path = new StringBuilder();
         path.append(writer.getName());
         JsonStreamContext sc = jgen.getOutputContext();
         if(sc != null) {
            sc = sc.getParent();
         }

         for(; sc != null; sc = sc.getParent()) {
            if(sc.getCurrentName() != null) {
               if(path.length() > 0) {
                  path.insert(0, ".");
               }

               path.insert(0, sc.getCurrentName());
            }
         }

         return path.toString();
      }

      public void serializeAsField(Object pojo, JsonGenerator jgen, SerializerProvider provider, PropertyWriter writer) throws Exception {
         String path = this.createPath(writer, jgen);
         if(!pattern.matcher(path).find()) {
            writer.serializeAsField(pojo, jgen, provider);
         } else if(!jgen.canOmitFields()) {
            writer.serializeAsOmittedField(pojo, jgen, provider);
         }

      }
   }

   @JsonFilter("secure filter")
   static class SecureFilterMixIn {
      SecureFilterMixIn() {
      }
   }

   public static class Data {
      @JsonProperty("dse_config")
      @JsonRawValue
      public final String dseConfig;
      @JsonProperty("cassandra_config")
      @JsonRawValue
      public final String cassandraConfig;
      @JsonProperty("jvm_version")
      public final String jvmVersion;
      @JsonProperty("jvm_vendor")
      public final String jvmVendor;
      @JsonProperty("jvm_memory_max")
      public final Long jvmMemoryMax;
      @JsonProperty("jvm_final_flags")
      public final List<String> jvmFlags;
      @JsonProperty("jvm_classpath")
      public final String jvmClasspath;
      @JsonProperty("jvm_properties")
      public final Map<String, String> jvmProperties;
      @JsonProperty("jvm_startup_time")
      public final Long jvmStartupTime;
      @JsonProperty("os")
      public final String os;
      @JsonProperty("hostname")
      public final String hostname;
      @JsonProperty("cpu_layout")
      public final List<Map<String, String>> cpuInfo;

      @JsonCreator
      @VisibleForTesting
      public Data(@JsonProperty("dse_config") Object dseConfig, @JsonProperty("cassandra_config") Object cassandraConfig, @JsonProperty("jvm_version") String jvmVersion, @JsonProperty("jvm_vendor") String jvmVendor, @JsonProperty("jvm_memory_max") Long jvmMemoryMax, @JsonProperty("jvm_final_flags") List<String> jvmFlags, @JsonProperty("jvm_classpath") String jvmClasspath, @JsonProperty("jvm_properties") Map<String, String> jvmProperties, @JsonProperty("jvm_startup_time") Long jvmStartupTime, @JsonProperty("os") String os, @JsonProperty("hostname") String hostname, @JsonProperty("cpu_layout") List<Map<String, String>> cpuInfo) {
         try {
            this.dseConfig = JacksonUtil.writeValueAsString(dseConfig);
         } catch (JacksonUtilException var15) {
            throw new IllegalArgumentException("Error creating dseConfig json", var15);
         }

         try {
            this.cassandraConfig = JacksonUtil.writeValueAsString(cassandraConfig);
         } catch (JacksonUtilException var14) {
            throw new IllegalArgumentException("Error creating cassandraConfig json", var14);
         }

         this.os = os;
         this.hostname = hostname;
         this.jvmStartupTime = jvmStartupTime;
         this.cpuInfo = cpuInfo;
         this.jvmProperties = jvmProperties;
         this.jvmMemoryMax = jvmMemoryMax;
         this.jvmVersion = jvmVersion;
         this.jvmFlags = jvmFlags;
         this.jvmClasspath = jvmClasspath;
         this.jvmVendor = jvmVendor;
      }

      private Data() {
         try {
            this.cassandraConfig = DSENodeConfiguration.SECURE_WRITER.writeValueAsString(DatabaseDescriptor.getRawConfig());
         } catch (JsonProcessingException var7) {
            throw new RuntimeException("Error generating cassandraConfig json", var7);
         }

         try {
            this.dseConfig = DSENodeConfiguration.SECURE_WRITER.writeValueAsString(DseConfig.getConfigRaw());
         } catch (JsonProcessingException var6) {
            throw new RuntimeException("Error generating dseConfig json", var6);
         }

         String hostname = "n/a";

         try {
            hostname = InetAddress.getLocalHost().getHostName();
         } catch (UnknownHostException var5) {
            DSENodeConfiguration.logger.info("Could not resolve hostname");
         }

         this.hostname = hostname;
         RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
         this.jvmVendor = runtime.getVmVendor();
         this.jvmVersion = runtime.getVmVersion();
         this.jvmClasspath = runtime.getClassPath();
         this.jvmFlags = runtime.getInputArguments();
         this.jvmMemoryMax = Long.valueOf(Runtime.getRuntime().maxMemory());
         this.jvmProperties = runtime.getSystemProperties();
         this.jvmStartupTime = Long.valueOf(runtime.getStartTime());
         this.os = FBUtilities.OPERATING_SYSTEM;
         List tmp = Collections.EMPTY_LIST;

         try {
            tmp = CpuInfo.loadCpuMap();
         } catch (IOError var8) {
            if(FBUtilities.isLinux) {
               DSENodeConfiguration.logger.warn("Error reading cpuInfo", var8);
            }
         }

         this.cpuInfo = tmp;
      }
   }
}
