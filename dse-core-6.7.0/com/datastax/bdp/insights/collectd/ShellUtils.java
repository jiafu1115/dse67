package com.datastax.bdp.insights.collectd;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.escape.Escapers.Builder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ShellUtils {
   private static final Logger logger = LoggerFactory.getLogger(ShellUtils.class);
   private static final Escaper SHELL_QUOTE_ESCAPER;

   ShellUtils() {
   }

   public static String[] split(CharSequence string) {
      List<String> tokens = new ArrayList();
      boolean escaping = false;
      char quoteChar = 32;
      boolean quoting = false;
      StringBuilder current = new StringBuilder();

      for(int i = 0; i < string.length(); ++i) {
         char c = string.charAt(i);
         if(escaping) {
            current.append(c);
            escaping = false;
         } else if(c == 92 && (!quoting || quoteChar != 39)) {
            escaping = true;
         } else if(quoting && c == quoteChar) {
            quoting = false;
         } else if(!quoting && (c == 39 || c == 34)) {
            quoting = true;
            quoteChar = c;
         } else if(!quoting && Character.isWhitespace(c)) {
            if(current.length() > 0) {
               tokens.add(current.toString());
               current = new StringBuilder();
            }
         } else {
            current.append(c);
         }
      }

      if(current.length() > 0) {
         tokens.add(current.toString());
      }

      return (String[])tokens.toArray(new String[0]);
   }

   public static String escape(String param) {
      return escape(param, false);
   }

   public static String escape(String param, boolean forceQuote) {
      String escapedQuotesParam = SHELL_QUOTE_ESCAPER.escape(param);
      return !forceQuote && !escapedQuotesParam.contains(" ")?escapedQuotesParam:"'" + escapedQuotesParam + "'";
   }

   public static List<String> wrapCommandWithBash(String command, boolean remoteCommand) {
      List<String> fullCmd = new ArrayList();
      fullCmd.add("/bin/bash");
      fullCmd.add("-o");
      fullCmd.add("pipefail");
      if(remoteCommand) {
         fullCmd.add("-l");
      }

      fullCmd.add("-c");
      if(remoteCommand) {
         String escapedCmd = escape(command, true);
         fullCmd.add(escapedCmd);
      } else {
         fullCmd.add(command);
      }

      return fullCmd;
   }

   public static Process executeShell(String command, Map<String, String> environment) {
      List<String> cmds = wrapCommandWithBash(command, false);
      logger.trace("Executing locally: {}, Env {}", String.join(" ", cmds), environment);
      ProcessBuilder pb = new ProcessBuilder(cmds);
      pb.environment().putAll(environment);

      try {
         return pb.start();
      } catch (IOException var5) {
         throw new RuntimeException(var5);
      }
   }

   public static <T> T executeShellWithHandlers(String command, ShellUtils.ThrowingBiFunction<BufferedReader, BufferedReader, T> handler, ShellUtils.ThrowingBiFunction<Integer, BufferedReader, T> errorHandler) throws IOException {
      return executeShellWithHandlers(command, handler, errorHandler, Collections.emptyMap());
   }

   public static <T> T executeShellWithHandlers(String command, ShellUtils.ThrowingBiFunction<BufferedReader, BufferedReader, T> handler, ShellUtils.ThrowingBiFunction<Integer, BufferedReader, T> errorHandler, Map<String, String> environment) throws IOException {
      Process ps = executeShell(command, environment);

      try {
         BufferedReader input = new BufferedReader(new InputStreamReader(ps.getInputStream()));
         Throwable var6 = null;

         Object var9;
         try {
            BufferedReader error = new BufferedReader(new InputStreamReader(ps.getErrorStream()));
            Throwable var8 = null;

            try {
               ps.waitFor();
               if(ps.exitValue() == 0) {
                  var9 = handler.apply(input, error);
                  return var9;
               }

               var9 = errorHandler.apply(Integer.valueOf(ps.exitValue()), error);
            } catch (Throwable var37) {
               var9 = var37;
               var8 = var37;
               throw var37;
            } finally {
               if(error != null) {
                  if(var8 != null) {
                     try {
                        error.close();
                     } catch (Throwable var36) {
                        var8.addSuppressed(var36);
                     }
                  } else {
                     error.close();
                  }
               }

            }
         } catch (Throwable var39) {
            var6 = var39;
            throw var39;
         } finally {
            if(input != null) {
               if(var6 != null) {
                  try {
                     input.close();
                  } catch (Throwable var35) {
                     var6.addSuppressed(var35);
                  }
               } else {
                  input.close();
               }
            }

         }

         return var9;
      } catch (InterruptedException var41) {
         throw new RuntimeException(var41);
      }
   }

   static {
      Builder builder = Escapers.builder();
      builder.addEscape('\'', "'\"'\"'");
      SHELL_QUOTE_ESCAPER = builder.build();
   }

   public interface ThrowingBiFunction<ArgType, Arg2Type, ResType> {
      ResType apply(ArgType var1, Arg2Type var2) throws IOException;
   }
}
