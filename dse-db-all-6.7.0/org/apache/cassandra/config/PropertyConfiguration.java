package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PropertyConfiguration {
   private static final Logger logger = LoggerFactory.getLogger(PropertyConfiguration.class);
   static PropertyConfiguration.PropertyConverter<String> stringConverterter = new PropertyConfiguration.PropertyConverter<String>() {
      public String convert(String property, String value) {
         return value;
      }
   };
   static PropertyConfiguration.PropertyConverter<Boolean> booleanConverter = new PropertyConfiguration.PropertyConverter<Boolean>() {
      public Boolean convert(String property, String value) {
         return Boolean.valueOf(Boolean.parseBoolean(value));
      }
   };
   static PropertyConfiguration.PropertyConverter<Integer> integerConverter = new PropertyConfiguration.PropertyConverter<Integer>() {
      public Integer convert(String property, String value) {
         try {
            return Integer.valueOf(Integer.parseInt(value));
         } catch (NumberFormatException var4) {
            throw new ConfigurationException(String.format("Invalid value for system property '%s': %s", new Object[]{property, String.format("expected integer value but got '%s'", new Object[]{value})}));
         }
      }
   };
   static PropertyConfiguration.PropertyConverter<Long> longConverter = new PropertyConfiguration.PropertyConverter<Long>() {
      public Long convert(String property, String value) {
         try {
            return Long.valueOf(Long.parseLong(value));
         } catch (NumberFormatException var4) {
            throw new ConfigurationException(String.format("Invalid value for system property '%s': %s", new Object[]{property, String.format("expected long value but got '%s'", new Object[]{value})}));
         }
      }
   };
   public static final PropertyConfiguration.PropertyCategory PUBLIC = new PropertyConfiguration.PropertyCategory(true);
   public static final PropertyConfiguration.PropertyCategory EXPERT = new PropertyConfiguration.PropertyCategory(Boolean.getBoolean("dse.config.log_expert_system_properties"));

   private PropertyConfiguration() {
   }

   public static String getString(String property) {
      return getString(property, (String)null, "");
   }

   public static String getString(String property, String defaultValue) {
      return getString(property, defaultValue, "");
   }

   public static String getString(String property, String defaultValue, String description) {
      return EXPERT.getString(property, defaultValue, description);
   }

   public static int getInteger(String property, int defaultValue) throws ConfigurationException {
      return getInteger(property, defaultValue, "");
   }

   public static int getInteger(String property, int defaultValue, String description) throws ConfigurationException {
      return EXPERT.getInteger(property, defaultValue, description);
   }

   public static long getLong(String property, long defaultValue) throws ConfigurationException {
      return getLong(property, defaultValue, "");
   }

   public static long getLong(String property, long defaultValue, String description) throws ConfigurationException {
      return EXPERT.getLong(property, defaultValue, description);
   }

   public static boolean getBoolean(String property) {
      return getBoolean(property, false, "");
   }

   public static boolean getBoolean(String property, boolean defaultValue) {
      return getBoolean(property, defaultValue, "");
   }

   public static boolean getBoolean(String property, boolean defaultValue, String description) throws ConfigurationException {
      return EXPERT.getBoolean(property, defaultValue, description);
   }

   private interface PropertyConverter<T> {
      T convert(String var1, String var2);
   }

   public static final class PropertyCategory {
      private final boolean log;

      public PropertyCategory(boolean log) {
         this.log = log;
      }

      private void log(String property, String value, Object defaultValue, String description) {
         if(this.log) {
            NoSpamLogger.log(PropertyConfiguration.logger, NoSpamLogger.Level.DEBUG, 1L, TimeUnit.MINUTES, String.format("Fetching property -D%s={} default={} {}", new Object[]{property}), new Object[]{value == null?defaultValue:value, defaultValue, description});
         }

      }

      public String getString(String property) {
         return this.getString(property, (String)null, "");
      }

      public String getString(String property, String defaultValue) {
         return this.getString(property, defaultValue, "");
      }

      public String getString(String property, String defaultValue, String description) {
         return (String)this.get(property, defaultValue, PropertyConfiguration.stringConverterter, description);
      }

      public int getInteger(String property, int defaultValue) throws ConfigurationException {
         return this.getInteger(property, defaultValue, "");
      }

      public int getInteger(String property, int defaultValue, String description) throws ConfigurationException {
         return ((Integer)this.get(property, Integer.valueOf(defaultValue), PropertyConfiguration.integerConverter, description)).intValue();
      }

      public long getLong(String property, long defaultValue) throws ConfigurationException {
         return this.getLong(property, defaultValue, "");
      }

      public long getLong(String property, long defaultValue, String description) throws ConfigurationException {
         return ((Long)this.get(property, Long.valueOf(defaultValue), PropertyConfiguration.longConverter, description)).longValue();
      }

      public boolean getBoolean(String property) {
         return this.getBoolean(property, false, "");
      }

      public boolean getBoolean(String property, boolean defaultValue) {
         return this.getBoolean(property, defaultValue, "");
      }

      public boolean getBoolean(String property, boolean defaultValue, String description) {
         return ((Boolean)this.get(property, Boolean.valueOf(defaultValue), PropertyConfiguration.booleanConverter, description)).booleanValue();
      }

      private <T> T get(String property, T defaultValue, PropertyConfiguration.PropertyConverter<T> converter, String description) {
         String value = System.getProperty(property);
         this.log(property, value, defaultValue, description);
         return value == null?defaultValue:converter.convert(property, value);
      }
   }
}
