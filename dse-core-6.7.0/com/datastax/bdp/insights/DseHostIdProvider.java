package com.datastax.bdp.insights;

import java.util.UUID;

public interface DseHostIdProvider {
   UUID getHostId();
}
