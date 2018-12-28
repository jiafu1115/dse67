package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(
   name = "listendpointspendinghints",
   description = "Print all the endpoints that this node has hints for"
)
public class ListEndpointsPendingHints extends NodeTool.NodeToolCmd {
   public ListEndpointsPendingHints() {
   }

   public void execute(NodeProbe probe) {
      Map<String, Map<String, String>> endpoints = probe.listEndpointsPendingHints();
      if(endpoints.isEmpty()) {
         System.out.println("This node does not have hints for other endpoints");
      } else {
         Map<String, String> endpointMap = probe.getEndpointMap();
         Map<String, String> simpleStates = probe.getFailureDetectorSimpleStates();
         EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
         TableBuilder tb = new TableBuilder();
         tb.add(new String[]{"Host ID", "Address", "Rack", "DC", "Status", "Total hints", "Total files", "Newest", "Oldest"});

         Entry entry;
         String endpoint;
         String address;
         String rack;
         String dc;
         String status;
         for(Iterator var8 = endpoints.entrySet().iterator(); var8.hasNext(); tb.add(new String[]{endpoint, address, rack, dc, status, (String)((Map)entry.getValue()).get("totalHints"), (String)((Map)entry.getValue()).get("totalFiles"), sdf.format(new Date(Long.parseLong((String)((Map)entry.getValue()).get("newest")))), sdf.format(new Date(Long.parseLong((String)((Map)entry.getValue()).get("oldest"))))})) {
            entry = (Entry)var8.next();
            endpoint = (String)entry.getKey();
            address = (String)endpointMap.get(endpoint);

            try {
               rack = epSnitchInfo.getRack(address);
               dc = epSnitchInfo.getDatacenter(address);
               status = (String)simpleStates.getOrDefault(InetAddress.getByName(address).toString(), "Unknown");
            } catch (UnknownHostException var16) {
               rack = "Unknown";
               dc = "Unknown";
               status = "Unknown";
            }
         }

         tb.printTo(System.out);
      }

   }
}
