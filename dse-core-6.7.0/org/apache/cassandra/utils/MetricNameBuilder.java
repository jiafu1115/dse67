package org.apache.cassandra.utils;

import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.util.MapBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

public class MetricNameBuilder {
   private Map<String, String> objectNameParts = new HashMap();
   private static final List<String> ObjectNameAllowedKeys = Arrays.asList(new String[]{"scope", "index", "metricType", "phase", "name", "subname"});
   private static final List<String> CMRScopeParts = Arrays.asList(new String[]{"index", "phase", "name", "subname"});

   public MetricNameBuilder() {
   }

   public MetricNameBuilder withScope(String scope) {
      this.objectNameParts.put("scope", scope);
      return this;
   }

   public MetricNameBuilder withMetricType(String metricType) {
      this.objectNameParts.put("metricType", metricType);
      return this;
   }

   public MetricNameBuilder withPhase(String phase) {
      this.objectNameParts.put("phase", phase);
      return this;
   }

   public MetricNameBuilder withIndex(String index) {
      this.objectNameParts.put("index", index);
      return this;
   }

   public MetricNameBuilder withName(String name) {
      this.objectNameParts.put("name", name);
      return this;
   }

   public MetricNameBuilder withSubname(String subname) {
      this.objectNameParts.put("subname", subname);
      return this;
   }

   public MetricName build() {
      ArrayList<String> objectNameKeys = new ArrayList<String>();
      ArrayList<String> objectNameValues = new ArrayList<String>();
      for (String key2 : ObjectNameAllowedKeys) {
         if (!this.objectNameParts.containsKey(key2)) continue;
         objectNameKeys.add(key2);
         objectNameValues.add(this.objectNameParts.get(key2));
      }
      String cmrScope = CMRScopeParts.stream().filter(key -> this.objectNameParts.containsKey(key)).map(key -> this.objectNameParts.get(key)).collect(Collectors.joining("."));
      String mbeanName = JMX.buildMBeanName(JMX.Type.METRICS, MapBuilder.<String,String>immutable().withKeys(objectNameKeys.toArray(new String[0])).withValues(objectNameValues.toArray(new String[0])).build());
      return new MetricName("com.datastax.bdp", this.objectNameParts.get("scope"), this.objectNameParts.get("metricType"), cmrScope, mbeanName);
   }
}
