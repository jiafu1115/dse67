package com.datastax.bdp.config;

import com.google.common.base.Objects;

public class InsightsOptions {
   public String data_dir;
   public String collectd_root;
   public String log_dir;
   public boolean service_options_enabled;

   public InsightsOptions() {
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         InsightsOptions that = (InsightsOptions)o;
         return Objects.equal(this.data_dir, that.data_dir) && Objects.equal(this.collectd_root, that.collectd_root) && Objects.equal(this.log_dir, that.log_dir) && Objects.equal(Boolean.valueOf(this.service_options_enabled), Boolean.valueOf(that.service_options_enabled));
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.data_dir, this.collectd_root, this.log_dir, Boolean.valueOf(this.service_options_enabled)});
   }
}
