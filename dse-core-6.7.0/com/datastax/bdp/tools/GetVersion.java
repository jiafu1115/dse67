package com.datastax.bdp.tools;

import com.datastax.bdp.db.util.ProductVersion;

public class GetVersion {
   public GetVersion() {
   }

   public static void main(String[] args) {
      System.out.println(getReleaseVersionString());
   }

   public static String getReleaseVersionString() {
      try {
         return ProductVersion.getDSEVersionString();
      } catch (Exception var1) {
         return "debug version";
      }
   }
}
