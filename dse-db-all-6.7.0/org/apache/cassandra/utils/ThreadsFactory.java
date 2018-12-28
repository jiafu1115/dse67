package org.apache.cassandra.utils;

import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.cassandra.concurrent.NamedThreadFactory;

public class ThreadsFactory {
   public ThreadsFactory() {
   }

   public static ExecutorService newSingleThreadedExecutor(String name) {
      return Executors.newSingleThreadExecutor(new NamedThreadFactory(name));
   }

   public static Thread newDaemonThread(Runnable r, String name) {
      return newThread(r, name, true);
   }

   public static Thread newThread(Runnable r, String name, boolean isDaemon) {
      Thread t = new FastThreadLocalThread(r, name);
      t.setDaemon(isDaemon);
      return t;
   }

   public static void addShutdownHook(Runnable r, String name) {
      Runtime.getRuntime().addShutdownHook(newThread(r, name, false));
   }
}
