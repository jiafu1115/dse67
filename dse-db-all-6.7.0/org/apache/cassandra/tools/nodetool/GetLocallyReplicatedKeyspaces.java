package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getlocallyreplicatedkeyspaces",
   description = "Print the locally replicated keyspaces"
)
public class GetLocallyReplicatedKeyspaces extends NodeTool.NodeToolCmd {
   public GetLocallyReplicatedKeyspaces() {
   }

   public void execute(NodeProbe probe) {
      System.out.printf("Locally replicated keyspaces: %s.%n", new Object[]{String.join(", ", probe.getLocallyReplicatedKeyspaces())});
   }
}
