package org.apache.cassandra.repair;

import java.net.InetAddress;
import org.apache.cassandra.utils.MerkleTrees;

public class TreeResponse {
   public final InetAddress endpoint;
   public final MerkleTrees trees;

   public TreeResponse(InetAddress endpoint, MerkleTrees trees) {
      this.endpoint = endpoint;
      this.trees = trees;
   }

   public TreeResponse withEndpoint(InetAddress newEndpoint) {
      return new TreeResponse(newEndpoint, this.trees);
   }
}
