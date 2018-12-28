package org.apache.cassandra.tools;

import com.datastax.bdp.db.util.ProductVersion;

public class GetVersion {
   public GetVersion() {
   }

   public static void main(String[] args) {
      System.out.println(ProductVersion.getReleaseVersionString());
   }
}
