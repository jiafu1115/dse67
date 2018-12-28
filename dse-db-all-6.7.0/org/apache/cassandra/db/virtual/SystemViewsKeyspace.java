package org.apache.cassandra.db.virtual;

import org.apache.cassandra.utils.UnmodifiableArrayList;

public final class SystemViewsKeyspace extends VirtualKeyspace {
   public static final String NAME = "system_views";

   private SystemViewsKeyspace(UnmodifiableArrayList<VirtualTable> views) {
      super("system_views", views);
   }

   public static SystemViewsKeyspace.Builder newBuilder() {
      return new SystemViewsKeyspace.Builder();
   }

   public static final class Builder {
      private final UnmodifiableArrayList.Builder<VirtualTable> views = UnmodifiableArrayList.builder();

      public Builder() {
      }

      public SystemViewsKeyspace.Builder addView(VirtualTable view) {
         this.views.add((Object)view);
         return this;
      }

      public SystemViewsKeyspace build() {
         return new SystemViewsKeyspace(this.views.build());
      }
   }
}
