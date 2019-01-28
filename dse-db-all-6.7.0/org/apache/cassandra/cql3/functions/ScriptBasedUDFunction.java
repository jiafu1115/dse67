package org.apache.cassandra.cql3.functions;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.ByteBuffer;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import jdk.nashorn.api.scripting.AbstractJSObject;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SetsFactory;
import org.apache.cassandra.utils.time.ApolloTime;

final class ScriptBasedUDFunction extends UDFunction {
   private static final ProtectionDomain protectionDomain;
   private static final AccessControlContext accessControlContext;
   private static final String[] allowedPackagesArray = new String[]{"", "com", "edu", "java", "javax", "javafx", "org", "java.lang", "java.lang.invoke", "java.lang.reflect", "java.nio.charset", "java.util", "java.util.concurrent", "javax.script", "sun.reflect", "jdk.internal.org.objectweb.asm.commons", "jdk.nashorn.internal.runtime", "jdk.nashorn.internal.runtime.linker", "java.math", "java.nio", "java.text", "com.google.common.base", "com.google.common.collect", "com.google.common.reflect", "com.datastax.driver.core", "com.datastax.driver.core.utils"};
   private static final Set<String> allowedPackages;
   private static final UDFExecutorService executor;
   private static final ClassFilter classFilter;
   private static final NashornScriptEngine scriptEngine;
   private final CompiledScript script;
   private final Object udfContextBinding;

   ScriptBasedUDFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes, AbstractType<?> returnType, boolean calledOnNullInput, String language, String body, boolean deterministic, boolean monotonic, List<ColumnIdentifier> monotonicOn) {
      super(name, argNames, argTypes, returnType, calledOnNullInput, language, body, deterministic, monotonic, monotonicOn);
      if("JavaScript".equalsIgnoreCase(language) && scriptEngine != null) {
         try {
            this.script = AccessController.doPrivileged((PrivilegedExceptionAction<CompiledScript>)() -> {
               return scriptEngine.compile(body);
            }, accessControlContext);
         } catch (PrivilegedActionException var13) {
            Throwable e = var13.getCause();
            logger.info("Failed to compile function '{}' for language {}: ", new Object[]{name, language, e});
            throw new InvalidRequestException(String.format("Failed to compile function '%s' for language %s: %s", new Object[]{name, language, e}));
         }

         this.udfContextBinding = new ScriptBasedUDFunction.UDFContextWrapper(this.udfContext);
      } else {
         throw new InvalidRequestException(String.format("Invalid language '%s' for function '%s'", new Object[]{language, name}));
      }
   }

   public boolean isAsyncExecution() {
      return DatabaseDescriptor.enableUserDefinedFunctionsThreads();
   }

   public ByteBuffer executeUserDefined(Arguments arguments) {
      int size = this.argTypes.size();
      Object[] params = new Object[size];

      for(int i = 0; i < size; ++i) {
         params[i] = arguments.get(i);
      }

      Object result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()?this.executeAsync(params):this.executeSameThread(params);
      return this.decompose(arguments.getProtocolVersion(), result);
   }

   protected Object executeAggregateUserDefined(Object firstParam, Arguments arguments) {
      Object[] params = new Object[this.argTypes.size()];
      params[0] = firstParam;

      for(int i = 1; i < params.length; ++i) {
         params[i] = arguments.get(i - 1);
      }

      return DatabaseDescriptor.enableUserDefinedFunctionsThreads()?this.executeAsync(params):this.executeSameThread(params);
   }

   private Object executeSameThread(Object[] parameters) {
      UDFQuotaState quotaState = this.newQuotaState();
      UDFExecResult result = new UDFExecResult();

      Object var4;
      try {
         this.executeScriptInternal(quotaState, result, parameters);
         var4 = result.result;
      } finally {
         this.checkExecFailures(result);
         this.checkExecWarnings(result);
      }

      return var4;
   }

   private Object executeAsync(Object[] parameters) {
      long t0 = ApolloTime.approximateNanoTime();
      long maxTime = 9223372036854775807L;
      ScriptBasedUDFunction.ScriptCallable scriptCallable = new ScriptBasedUDFunction.ScriptCallable(parameters);
      Future future = executor.submit(scriptCallable);

      Object var14;
      while(true) {
         boolean var26 = false;

         try {
            var26 = true;
            UDFQuotaState qs = scriptCallable.quotaState;
            long t;
            if(qs != null) {
               maxTime = qs.failCpuTimeNanos;
               t = this.checkAndFail(qs);
            } else {
               t = 0L;
            }

            try {
               long wait = Math.max(10000L, maxTime - t);
               Object r = future.get(wait, TimeUnit.NANOSECONDS);
               if(qs == null) {
                  qs = scriptCallable.quotaState;
               }

               this.checkAndFail(qs);
               var14 = r;
               var26 = false;
               break;
            } catch (TimeoutException var27) {
               ;
            } catch (InterruptedException var28) {
               Thread.currentThread().interrupt();
               throw new RuntimeException(var28);
            } catch (ExecutionException var29) {
               Throwable c = var29.getCause();
               if(c instanceof RuntimeException) {
                  throw (RuntimeException)c;
               }

               throw new RuntimeException(c);
            }
         } finally {
            if(var26) {
               if(scriptCallable.execResult != null) {
                  this.checkExecWarnings(scriptCallable.execResult);
               }

               long t = ApolloTime.approximateNanoTime() - t0;
               if(t > DatabaseDescriptor.getUserDefinedFunctionWarnCpuTimeNanos()) {
                  String warn = String.format("Total script user defined function %s execution took longer than %dµs CPU time (%dµs)", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionWarnCpuTimeMicros()), Long.valueOf(TimeUnit.NANOSECONDS.toMicros(t))});
                  logger.warn(warn);
                  ClientWarn.instance.warn(warn);
               }

            }
         }
      }

      if(scriptCallable.execResult != null) {
         this.checkExecWarnings(scriptCallable.execResult);
      }

      long t = ApolloTime.approximateNanoTime() - t0;
      if(t > DatabaseDescriptor.getUserDefinedFunctionWarnCpuTimeNanos()) {
         String warn = String.format("Total script user defined function %s execution took longer than %dµs CPU time (%dµs)", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionWarnCpuTimeMicros()), Long.valueOf(TimeUnit.NANOSECONDS.toMicros(t))});
         logger.warn(warn);
         ClientWarn.instance.warn(warn);
      }

      return var14;
   }

   private long checkAndFail(UDFQuotaState qs) {
      long t = qs.cpuTimeNanos();
      long heap = qs.allocatedBytes();
      if(t > qs.failCpuTimeNanos) {
         throw this.failTime(t);
      } else if(heap > qs.failBytes) {
         throw this.failHeap(heap);
      } else {
         return t;
      }
   }

   private FunctionExecutionException failHeap(long heap) {
      TimeoutException cause = new TimeoutException(String.format("User defined function %s allocated more than %dMB (%dMB)%s", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionFailHeapMb()), Long.valueOf(heap / 1024L / 1024L), DatabaseDescriptor.getUserFunctionFailPolicy() == Config.UserFunctionFailPolicy.ignore?"":" - will stop Cassandra VM"}));
      FunctionExecutionException fe = FunctionExecutionException.create(this, cause);
      JVMStabilityInspector.userFunctionTimeout(cause);
      return fe;
   }

   private FunctionExecutionException failTime(long t) {
      TimeoutException cause = new TimeoutException(String.format("User defined function %s consumed more than %dµs CPU time (%dµs)%s", new Object[]{this, Long.valueOf(DatabaseDescriptor.getUserDefinedFunctionFailCpuTimeMicros()), Long.valueOf(TimeUnit.NANOSECONDS.toMicros(t)), DatabaseDescriptor.getUserFunctionFailPolicy() == Config.UserFunctionFailPolicy.ignore?"":" - will stop Cassandra VM"}));
      FunctionExecutionException fe = FunctionExecutionException.create(this, cause);
      JVMStabilityInspector.userFunctionTimeout(cause);
      return fe;
   }

   private void executeScriptInternal(UDFQuotaState quotaState, UDFExecResult result, Object[] params) {
      ScriptContext scriptContext = new SimpleScriptContext();
      Bindings bindings = scriptContext.getBindings(100);
      bindings.put("javax.script.filename", this.name.toString());

      for(int i = 0; i < params.length; ++i) {
         bindings.put(((ColumnIdentifier)this.argNames.get(i)).toString(), params[i]);
      }

      bindings.put("udfContext", this.udfContextBinding);
      preExecHook.run();

      try {
         ThreadAwareSecurityManager.enterSecureSection(quotaState, allowedPackages).onStart();
         Object rval = this.script.eval(scriptContext);
         if(rval != null) {
            result.result = convert(rval, this.resultType);
            return;
         }
      } catch (ScriptException var10) {
         throw new RuntimeException(var10);
      } finally {
         ThreadAwareSecurityManager.leaveSecureSection().afterExec(result);
         bindings.clear();
         postExecHook.run();
      }

   }

   private static Object convert(Object obj, UDFDataType udfDataType) {
      Class<?> resultType = obj.getClass();
      Class<?> type = udfDataType.toJavaClass();
      if(type.isAssignableFrom(resultType)) {
         return obj;
      } else if(obj instanceof Number) {
         Number number = (Number)obj;
         return type == Integer.class?Integer.valueOf(number.intValue()):(type == Long.class?Long.valueOf(number.longValue()):(type == Short.class?Short.valueOf(number.shortValue()):(type == Byte.class?Byte.valueOf(number.byteValue()):(type == Float.class?Float.valueOf(number.floatValue()):(type == Double.class?Double.valueOf(number.doubleValue()):(type == BigInteger.class?(number instanceof BigDecimal?((BigDecimal)number).toBigInteger():(!(number instanceof Double) && !(number instanceof Float)?BigInteger.valueOf(number.longValue()):(new BigDecimal(number.toString())).toBigInteger())):new BigDecimal(number.toString())))))));
      } else {
         throw new InvalidTypeException("Invalid value for CQL type " + udfDataType.toDataType().getName());
      }
   }

   static {
      allowedPackages = SetsFactory.setFromArray(allowedPackagesArray);
      executor = new UDFExecutorService(new NamedThreadFactory("UserDefinedScriptFunctions", 1, udfClassLoader), "userscripts");
      classFilter = (clsName) -> {
         return isSecureResource(clsName.replace('.', '/') + ".class");
      };
      ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
      ScriptEngine engine = scriptEngineManager.getEngineByName("nashorn");
      NashornScriptEngineFactory factory = engine != null?(NashornScriptEngineFactory)engine.getFactory():null;
      scriptEngine = factory != null?(NashornScriptEngine)factory.getScriptEngine(new String[]{"--global-per-engine"}, udfClassLoader, classFilter):null;

      try {
         protectionDomain = new ProtectionDomain(new CodeSource(new URL("udf", "localhost", 0, "/script", new URLStreamHandler() {
            protected URLConnection openConnection(URL u) {
               return null;
            }
         }), (Certificate[])null), ThreadAwareSecurityManager.noPermissions);
      } catch (MalformedURLException var4) {
         throw new RuntimeException(var4);
      }

      accessControlContext = new AccessControlContext(new ProtectionDomain[]{protectionDomain});
   }

   private final class ScriptCallable implements Callable<Object> {
      private final Object[] parameters;
      volatile UDFQuotaState quotaState;
      final UDFExecResult execResult = new UDFExecResult();

      ScriptCallable(Object[] parameters) {
         this.parameters = parameters;
      }

      public Object call() {
         this.quotaState = ScriptBasedUDFunction.this.newQuotaState();

         Object var1;
         try {
            ScriptBasedUDFunction.this.executeScriptInternal(this.quotaState, this.execResult, this.parameters);
            var1 = this.execResult.result;
         } finally {
            ScriptBasedUDFunction.this.executed = true;
         }

         return var1;
      }
   }

   private static final class UDFContextWrapper extends AbstractJSObject {
      protected final UDFContext udfContext;
      private final AbstractJSObject fRetUDT;
      private final AbstractJSObject fArgUDT;
      private final AbstractJSObject fRetTup;
      private final AbstractJSObject fArgTup;

      UDFContextWrapper(final UDFContext udfContext) {
         this.udfContext = udfContext;
         this.fRetUDT = new AbstractJSObject() {
            public Object call(Object thiz, Object... args) {
               if(args != null && args.length != 0) {
                  throw new IllegalArgumentException("newReturnUDTValue() has no arguments");
               } else {
                  return udfContext.newReturnUDTValue();
               }
            }
         };
         this.fArgUDT = new AbstractJSObject() {
            public Object call(Object thiz, Object... args) {
               if(args != null && args.length == 1) {
                  return args[0] instanceof String?udfContext.newArgUDTValue((String)args[0]):(args[0] instanceof Number?udfContext.newArgUDTValue(((Number)args[0]).intValue()):super.call(thiz, args));
               } else {
                  throw new IllegalArgumentException("newArgUDTValue(argNameOrIndex) requires the argument name or index as its argument");
               }
            }
         };
         this.fRetTup = new AbstractJSObject() {
            public Object call(Object thiz, Object... args) {
               if(args != null && args.length != 0) {
                  throw new IllegalArgumentException("newReturnTupleValue() has no arguments");
               } else {
                  return udfContext.newReturnTupleValue();
               }
            }
         };
         this.fArgTup = new AbstractJSObject() {
            public Object call(Object thiz, Object... args) {
               if(args != null && args.length == 1) {
                  return args[0] instanceof String?udfContext.newArgTupleValue((String)args[0]):(args[0] instanceof Number?udfContext.newArgTupleValue(((Number)args[0]).intValue()):super.call(thiz, args));
               } else {
                  throw new IllegalArgumentException("newArgTupleValue(argNameOrIndex) requires the argument name or index as its argument");
               }
            }
         };
      }

      public Object getMember(String name) {
         byte var3 = -1;
         switch(name.hashCode()) {
         case -1560099905:
            if(name.equals("newArgTupleValue")) {
               var3 = 3;
            }
            break;
         case 93422524:
            if(name.equals("newReturnUDTValue")) {
               var3 = 0;
            }
            break;
         case 403494754:
            if(name.equals("newArgUDTValue")) {
               var3 = 1;
            }
            break;
         case 1108197785:
            if(name.equals("newReturnTupleValue")) {
               var3 = 2;
            }
         }

         switch(var3) {
         case 0:
            return this.fRetUDT;
         case 1:
            return this.fArgUDT;
         case 2:
            return this.fRetTup;
         case 3:
            return this.fArgTup;
         default:
            return super.getMember(name);
         }
      }
   }
}
