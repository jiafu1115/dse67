package com.datastax.bdp.gms;

import java.util.Collections;
import java.util.Map;

public final class DseEndpointState {
   public static final DseEndpointState nullDseEndpointState = new DseEndpointState();
   private Map<String, DseState.CoreIndexingStatus> coreIndexingStatus;
   private double nodeHealth;
   private boolean active;
   private boolean blacklisted;

   public DseEndpointState() {
   }

   public Map<String, DseState.CoreIndexingStatus> getCoreIndexingStatus() {
      return this.coreIndexingStatus;
   }

   public void setCoreIndexingStatus(Map<String, DseState.CoreIndexingStatus> coreIndexingStatus) {
      this.coreIndexingStatus = coreIndexingStatus;
   }

   public double getNodeHealth() {
      return this.nodeHealth;
   }

   public void setNodeHealth(double nodeHealth) {
      this.nodeHealth = nodeHealth;
   }

   public boolean isActive() {
      return this.active;
   }

   public void setActive(boolean active) {
      this.active = active;
   }

   public boolean isBlacklisted() {
      return this.blacklisted;
   }

   public void setBlacklisted(boolean blacklisted) {
      this.blacklisted = blacklisted;
   }

   static {
      nullDseEndpointState.coreIndexingStatus = Collections.emptyMap();
   }
}
