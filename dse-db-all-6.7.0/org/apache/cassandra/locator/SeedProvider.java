package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.List;

public interface SeedProvider {
   List<InetAddress> getSeeds();
}
