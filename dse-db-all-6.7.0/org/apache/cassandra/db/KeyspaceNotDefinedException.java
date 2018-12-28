package org.apache.cassandra.db;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class KeyspaceNotDefinedException extends InvalidRequestException {
   public KeyspaceNotDefinedException(String why) {
      super(why);
   }
}
