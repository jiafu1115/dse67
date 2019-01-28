package org.apache.cassandra.cql3.functions;

import com.google.common.annotations.VisibleForTesting;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.time.ApolloTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UDFunction extends AbstractFunction implements ScalarFunction {
   protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);
   protected final List<ColumnIdentifier> argNames;
   protected final String language;
   protected final String body;
   protected final boolean deterministic;
   protected final boolean monotonic;
   protected final List<ColumnIdentifier> monotonicOn;
   protected final List<UDFDataType> argumentTypes;
   protected final UDFDataType resultType;
   protected final boolean calledOnNullInput;
   protected final UDFContext udfContext;
   private static final String[] whitelistedPatterns = new String[]{"com/datastax/driver/core/", "com/google/common/reflect/TypeToken", "java/io/", "java/lang/", "java/math/", "java/net/InetAddress.class", "java/net/Inet4Address.class", "java/net/Inet6Address.class", "java/net/UnknownHostException.class", "java/net/NetworkInterface.class", "java/net/SocketException.class", "java/nio/Buffer.class", "java/nio/ByteBuffer.class", "java/text/", "java/time/", "java/util/", "org/apache/cassandra/cql3/functions/Arguments.class", "org/apache/cassandra/cql3/functions/UDFDataType.class", "org/apache/cassandra/cql3/functions/JavaUDF.class", "org/apache/cassandra/cql3/functions/UDFContext.class", "org/apache/cassandra/exceptions/", "org/apache/cassandra/transport/ProtocolVersion.class"};
   private static final String[] blacklistedPatterns = new String[]{"com/datastax/driver/core/AbstractSession.class", "com/datastax/driver/core/Cluster.class", "com/datastax/driver/core/Metrics.class", "com/datastax/driver/core/NettyOptions.class", "com/datastax/driver/core/Session.class", "com/datastax/driver/core/Statement.class", "com/datastax/driver/core/TimestampGenerator.class", "java/io/Console.class", "java/io/File", "java/io/RandomAccessFile.class", "java/lang/Compiler.class", "java/lang/InheritableThreadLocal.class", "java/lang/Package.class", "java/lang/Process.class", "java/lang/ProcessBuilder.class", "java/lang/ProcessEnvironment.class", "java/lang/ProcessImpl.class", "java/lang/Runnable.class", "java/lang/Runtime.class", "java/lang/Shutdown.class", "java/lang/Thread.class", "java/lang/ThreadGroup.class", "java/lang/ThreadLocal.class", "java/lang/instrument/", "java/lang/invoke/", "java/lang/management/", "java/lang/ref/", "java/lang/reflect/", "java/util/ServiceLoader.class", "java/util/Timer.class", "java/util/concurrent/", "java/util/jar/", "java/util/logging/", "java/util/prefs/", "java/util/spi/", "java/util/stream/", "java/util/zip/"};
   @VisibleForTesting
   static Runnable preExecHook = () -> {
   };
   @VisibleForTesting
   static Runnable postExecHook = () -> {
   };
   static final ClassLoader udfClassLoader = new UDFunction.UDFClassLoader();
   private static final long INITIAL_EXECUTION_GRACE_TIME;
   protected volatile boolean executed;

   static boolean isSecureResource(String resource) {
      while(resource.startsWith("/")) {
         resource = resource.substring(1);
      }

      String[] var1 = whitelistedPatterns;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         String white = var1[var3];
         if(resource.startsWith(white)) {
            String[] var5 = blacklistedPatterns;
            int var6 = var5.length;

            for(int var7 = 0; var7 < var6; ++var7) {
               String black = var5[var7];
               if(resource.startsWith(black)) {
                  logger.trace("access denied: resource {}", resource);
                  return false;
               }
            }

            return true;
         }
      }

      logger.trace("access denied: resource {}", resource);
      return false;
   }

   protected UDFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      super(name, argTypes, returnType);

      assert SetsFactory.setFromCollection(argNames).size() == argNames.size() : "duplicate argument names";

      this.argNames = argNames;
      this.language = language;
      this.body = body;
      this.deterministic = deterministic;
      this.monotonic = monotonic;
      this.monotonicOn = monotonicOn;
      this.argumentTypes = UDFDataType.wrap(argTypes, !calledOnNullInput);
      this.resultType = UDFDataType.wrap(returnType, !calledOnNullInput);
      this.calledOnNullInput = calledOnNullInput;
      KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(name.keyspace);
      this.udfContext = new UDFContextImpl(argNames, this.argumentTypes, this.resultType, keyspaceMetadata);
   }

   public Arguments newArguments(ProtocolVersion version) {
      return FunctionArguments.newInstanceForUdf(version, this.argumentTypes);
   }

   public static UDFunction create(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      assertUdfsEnabled(language);
      byte var11 = -1;
      switch(language.hashCode()) {
      case 3254818:
         if(language.equals("java")) {
            var11 = 0;
         }
      default:
         switch(var11) {
         case 0:
            return new JavaBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, body, deterministic, monotonic, monotonicOn);
         default:
            return new ScriptBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, monotonicOn);
         }
      }
   }

   public static UDFunction createBrokenFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn, final InvalidRequestException reason) {
      return new UDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, monotonicOn) {
         protected Object executeAggregateUserDefined(Object state, Arguments arguments) {
            throw this.broken();
         }

         public ByteBuffer executeUserDefined(Arguments arguments) {
            throw this.broken();
         }

         private InvalidRequestException broken() {
            return new InvalidRequestException(String.format("Function '%s' exists but hasn't been loaded successfully for the following reason: %s. Please see the server log for details", new Object[]{this, reason.getMessage()}));
         }
      };
   }

   public boolean isDeterministic() {
      return this.deterministic;
   }

   public boolean isMonotonic() {
      return this.monotonic;
   }

   public boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters) {
      assert partialParameters.size() == this.argNames.size();

      if(!this.monotonic) {
         for(int i = 0; i < partialParameters.size(); ++i) {
            ByteBuffer partialParameter = (ByteBuffer)partialParameters.get(i);
            if(partialParameter == Function.UNRESOLVED) {
               ColumnIdentifier unresolvedArgumentName = (ColumnIdentifier)this.argNames.get(i);
               if(!this.monotonicOn.contains(unresolvedArgumentName)) {
                  return false;
               }
            }
         }
      }

      return true;
   }

   public final ByteBuffer execute(Arguments arguments) {
      assertUdfsEnabled(this.language);
      if(!this.isCallableWrtNullable(arguments)) {
         return null;
      } else {
         long tStart = ApolloTime.approximateNanoTime();

         try {
            ByteBuffer result = this.executeUserDefined(arguments);
            Tracing.trace("Executed UDF {} in {}μs", this.name(), Long.valueOf((ApolloTime.approximateNanoTime() - tStart) / 1000L));
            return result;
         } catch (InvalidRequestException var5) {
            throw var5;
         } catch (Throwable var6) {
            logger.trace("Invocation of user-defined function '{}' failed", this, var6);
            if(var6 instanceof VirtualMachineError) {
               throw (VirtualMachineError)var6;
            } else {
               throw FunctionExecutionException.create(this, var6);
            }
         }
      }
   }

   public final Object executeForAggregate(Object state, Arguments arguments) {
      assertUdfsEnabled(this.language);
      if((this.calledOnNullInput || state != null) && this.isCallableWrtNullable(arguments)) {
         long tStart = ApolloTime.approximateNanoTime();

         try {
            Object result = this.executeAggregateUserDefined(state, arguments);
            Tracing.trace("Executed UDF {} in {}μs", this.name(), Long.valueOf((ApolloTime.approximateNanoTime() - tStart) / 1000L));
            return result;
         } catch (InvalidRequestException var6) {
            throw var6;
         } catch (Throwable var7) {
            logger.debug("Invocation of user-defined function '{}' failed", this, var7);
            if(var7 instanceof VirtualMachineError) {
               throw (VirtualMachineError)var7;
            } else {
               throw FunctionExecutionException.create(this, var7);
            }
         }
      } else {
         return null;
      }
   }

   public static void assertUdfsEnabled(String language) {
      if(!DatabaseDescriptor.enableUserDefinedFunctions()) {
         throw new InvalidRequestException("User-defined functions are disabled in cassandra.yaml - set enable_user_defined_functions=true to enable");
      } else if(!"java".equalsIgnoreCase(language) && !DatabaseDescriptor.enableScriptedUserDefinedFunctions()) {
         throw new InvalidRequestException("Scripted user-defined functions are disabled in cassandra.yaml - set enable_scripted_user_defined_functions=true to enable if you are aware of the security risks");
      }
   }

   protected abstract ByteBuffer executeUserDefined(Arguments var1);

   protected abstract Object executeAggregateUserDefined(Object var1, Arguments var2);

   protected UDFQuotaState newQuotaState() {
      long maxTime = DatabaseDescriptor.getUserDefinedFunctionFailCpuTimeNanos();
      if(!this.executed) {
         maxTime += INITIAL_EXECUTION_GRACE_TIME;
      }

      return new UDFQuotaState(maxTime, DatabaseDescriptor.getUserDefinedFunctionWarnCpuTimeNanos(), DatabaseDescriptor.getUserDefinedFunctionFailHeapMb(), DatabaseDescriptor.getUserDefinedFunctionWarnHeapMb());
   }

   void checkExecWarnings(UDFExecResult result) {
      String warn;
      if(result.warnCpuTimeExceeded) {
         warn = String.format("User defined function %s consumed more than %dµs CPU time (%dµs)", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionWarnCpuTimeMicros()), Long.valueOf(TimeUnit.NANOSECONDS.toMicros(result.cpuTime))});
         logger.warn(warn);
         ClientWarn.instance.warn(warn);
      }

      if(result.warnAllocatedBytesExceeded) {
         warn = String.format("User defined function %s allocated more than %dMB heap (%dMB)", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionWarnHeapMb()), Long.valueOf(result.allocatedBytes / 1024L / 1024L)});
         logger.warn(warn);
         ClientWarn.instance.warn(warn);
      }

   }

   void checkExecFailures(UDFExecResult result) {
      if(result.failCpuTimeExceeded) {
         TimeoutException cause = new TimeoutException(String.format("User defined function %s consumed more than %dµs CPU time (%dµs)", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionFailCpuTimeMicros()), Long.valueOf(TimeUnit.NANOSECONDS.toMicros(result.cpuTime))}));
         throw FunctionExecutionException.create(this, cause);
      } else if(result.failAllocatedBytesExceeded) {
         RuntimeException cause = new RuntimeException(String.format("User defined function %s allocated more than %dMB heap (%dMB)", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionFailHeapMb()), Long.valueOf(result.allocatedBytes / 1024L / 1024L)}));
         throw FunctionExecutionException.create(this, cause);
      }
   }

   public boolean isCallableWrtNullable(Arguments arguments) {
      return this.calledOnNullInput || !arguments.containsNulls();
   }

   public boolean isAggregate() {
      return false;
   }

   public boolean isNative() {
      return false;
   }

   public boolean isCalledOnNullInput() {
      return this.calledOnNullInput;
   }

   public List<ColumnIdentifier> argNames() {
      return this.argNames;
   }

   public String body() {
      return this.body;
   }

   public String language() {
      return this.language;
   }

   public List<ColumnIdentifier> monotonicOn() {
      return this.monotonicOn;
   }

   protected ByteBuffer decompose(ProtocolVersion protocolVersion, Object value) {
      return this.resultType.decompose(protocolVersion, value);
   }

   public boolean equals(Object o) {
      if(!(o instanceof UDFunction)) {
         return false;
      } else {
         UDFunction that = (UDFunction)o;
         return Objects.equals(this.name, that.name) && Objects.equals(this.argNames, that.argNames) && Functions.typesMatch(this.argTypes, that.argTypes) && Functions.typesMatch(this.returnType, that.returnType) && Objects.equals(this.language, that.language) && Objects.equals(this.body, that.body);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.name, Integer.valueOf(Functions.typeHashCode(this.argTypes)), this.returnType, this.language, this.body});
   }

   static {
      INITIAL_EXECUTION_GRACE_TIME = TimeUnit.MILLISECONDS.toNanos(PropertyConfiguration.getLong("dse.core.udf.script.initial-grace-time-millis", 2000L));
   }

   private static class UDFClassLoader extends ClassLoader {
      static final ClassLoader insecureClassLoader = Thread.currentThread().getContextClassLoader();

      private UDFClassLoader() {
      }

      public URL getResource(String name) {
         return !UDFunction.isSecureResource(name)?null:insecureClassLoader.getResource(name);
      }

      protected URL findResource(String name) {
         return this.getResource(name);
      }

      public Enumeration<URL> getResources(String name) {
         return Collections.emptyEnumeration();
      }

      protected Class<?> findClass(String name) throws ClassNotFoundException {
         if(!UDFunction.isSecureResource(name.replace('.', '/') + ".class")) {
            throw new ClassNotFoundException(name);
         } else {
            return insecureClassLoader.loadClass(name);
         }
      }

      public Class<?> loadClass(String name) throws ClassNotFoundException {
         if(!UDFunction.isSecureResource(name.replace('.', '/') + ".class")) {
            throw new ClassNotFoundException(name);
         } else {
            return super.loadClass(name);
         }
      }
   }
}
