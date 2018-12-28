package org.apache.cassandra.cql3.functions;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.cassandra.utils.SetsFactory;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

public class JavaUDFByteCodeVerifier {
   public static final String JAVA_UDF_NAME = JavaUDF.class.getName().replace('.', '/');
   public static final String OBJECT_NAME = Object.class.getName().replace('.', '/');
   public static final String CTOR_SIG = "(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V";
   private final Set<String> disallowedClasses = SetsFactory.newSet();
   private final Multimap<String, String> disallowedMethodCalls = HashMultimap.create();
   private final List<String> disallowedPackages = new ArrayList();

   public JavaUDFByteCodeVerifier() {
      this.addDisallowedMethodCall(OBJECT_NAME, "clone");
      this.addDisallowedMethodCall(OBJECT_NAME, "finalize");
      this.addDisallowedMethodCall(OBJECT_NAME, "notify");
      this.addDisallowedMethodCall(OBJECT_NAME, "notifyAll");
      this.addDisallowedMethodCall(OBJECT_NAME, "wait");
   }

   public JavaUDFByteCodeVerifier addDisallowedClass(String clazz) {
      this.disallowedClasses.add(clazz);
      return this;
   }

   public JavaUDFByteCodeVerifier addDisallowedMethodCall(String clazz, String method) {
      this.disallowedMethodCalls.put(clazz, method);
      return this;
   }

   public JavaUDFByteCodeVerifier addDisallowedPackage(String pkg) {
      this.disallowedPackages.add(pkg);
      return this;
   }

   public JavaUDFByteCodeVerifier.ClassAndErrors verifyAndInstrument(String clsName, byte[] bytes) {
      final String clsNameSl = clsName.replace('.', '/');
      ClassWriter classWriter = new ClassWriter(3);
      final Set<String> errors = new TreeSet();
      ClassVisitor classVisitor = new ClassVisitor(327680, classWriter) {
         public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            errors.add("field declared: " + name);
            return super.visitField(access, name, desc, signature, value);
         }

         public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if((access & 32) != 0) {
               errors.add("synchronized method " + name);
            }

            MethodVisitor delegate = super.visitMethod(access, name, desc, signature, exceptions);
            if("<init>".equals(name) && "(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V".equals(desc)) {
               if(1 != access) {
                  errors.add("constructor not public");
               }

               return new JavaUDFByteCodeVerifier.ConstructorVisitor(errors, delegate);
            } else if("executeImpl".equals(name) && "(Lorg/apache/cassandra/cql3/functions/Arguments;)Ljava/nio/ByteBuffer;".equals(desc)) {
               if(4 != access) {
                  errors.add("executeImpl not protected");
               }

               return JavaUDFByteCodeVerifier.this.new ExecuteImplVisitor(errors, delegate, ByteBuffer.class);
            } else if("executeAggregateImpl".equals(name) && "(Ljava/lang/Object;Lorg/apache/cassandra/cql3/functions/Arguments;)Ljava/lang/Object;".equals(desc)) {
               if(4 != access) {
                  errors.add("executeAggregateImpl not protected");
               }

               return JavaUDFByteCodeVerifier.this.new ExecuteImplVisitor(errors, delegate, Object.class);
            } else if("<clinit>".equals(name)) {
               errors.add("static initializer declared");
               return delegate;
            } else {
               Method m = new Method(name, desc);
               Type rt = m.getReturnType();
               Class<?> returnClass = JavaUDFByteCodeVerifier.this.classForAsmType(name, desc, rt, errors);
               return JavaUDFByteCodeVerifier.this.new ExecuteImplVisitor(errors, delegate, returnClass);
            }
         }

         public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            if(!JavaUDFByteCodeVerifier.JAVA_UDF_NAME.equals(superName)) {
               errors.add("class does not extend " + JavaUDF.class.getName());
            }

            if(access != 49) {
               errors.add("class not public final");
            }

            super.visit(version, access, name, signature, superName, interfaces);
         }

         public void visitInnerClass(String name, String outerName, String innerName, int access) {
            if(clsNameSl.equals(outerName)) {
               errors.add("class declared as inner class");
            }

            super.visitInnerClass(name, outerName, innerName, access);
         }
      };
      ClassReader classReader = new ClassReader(bytes);
      classReader.accept(classVisitor, 0);
      byte[] newByteCode = classWriter.toByteArray();
      return new JavaUDFByteCodeVerifier.ClassAndErrors(newByteCode, errors);
   }

   private Class<?> classForAsmType(String name, String desc, Type rt, Set<String> errors) {
      try {
         String className = rt.getClassName();
         byte var7 = -1;
         switch(className.hashCode()) {
         case -1325958191:
            if(className.equals("double")) {
               var7 = 7;
            }
            break;
         case 104431:
            if(className.equals("int")) {
               var7 = 4;
            }
            break;
         case 3039496:
            if(className.equals("byte")) {
               var7 = 1;
            }
            break;
         case 3052374:
            if(className.equals("char")) {
               var7 = 2;
            }
            break;
         case 3327612:
            if(className.equals("long")) {
               var7 = 5;
            }
            break;
         case 3625364:
            if(className.equals("void")) {
               var7 = 0;
            }
            break;
         case 97526364:
            if(className.equals("float")) {
               var7 = 6;
            }
            break;
         case 109413500:
            if(className.equals("short")) {
               var7 = 3;
            }
         }

         switch(var7) {
         case 0:
            return Void.TYPE;
         case 1:
            return Byte.TYPE;
         case 2:
            return Character.TYPE;
         case 3:
            return Short.TYPE;
         case 4:
            return Integer.TYPE;
         case 5:
            return Long.TYPE;
         case 6:
            return Float.TYPE;
         case 7:
            return Double.TYPE;
         default:
            Class<?> returnClass = Class.forName(className);
            errors.add("not allowed method declared: " + name + desc);
            return returnClass;
         }
      } catch (ClassNotFoundException var8) {
         throw new RuntimeException(var8);
      }
   }

   public boolean checkMethodCall(String owner, String name) {
      return true;
   }

   public static class ClassAndErrors {
      public final byte[] bytecode;
      public final Set<String> errors;

      ClassAndErrors(byte[] bytecode, Set<String> errors) {
         this.bytecode = bytecode;
         this.errors = errors;
      }
   }

   private static class ConstructorVisitor extends MethodVisitor {
      private final Set<String> errors;

      ConstructorVisitor(Set<String> errors, MethodVisitor delegate) {
         super(327680, delegate);
         this.errors = errors;
      }

      public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
         this.errors.add("Use of invalid method instruction in constructor");
         super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
      }

      public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
         if(183 != opcode || !JavaUDFByteCodeVerifier.JAVA_UDF_NAME.equals(owner) || !"<init>".equals(name) || !"(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V".equals(desc)) {
            this.errors.add("initializer declared");
         }

         super.visitMethodInsn(opcode, owner, name, desc, itf);
      }

      public void visitInsn(int opcode) {
         if(177 != opcode) {
            this.errors.add("initializer declared");
         }

         super.visitInsn(opcode);
      }
   }

   private class ExecuteImplVisitor extends MethodVisitor {
      private final Set<String> errors;
      private final Class<?> returnType;

      ExecuteImplVisitor(Set<String> var1, MethodVisitor errors, Class<?> delegate) {
         super(327680, delegate);
         this.errors = errors;
         this.returnType = returnType;
      }

      public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
         if(JavaUDFByteCodeVerifier.this.disallowedClasses.contains(owner)) {
            this.errorDisallowed(owner, name);
         }

         Collection<String> disallowed = JavaUDFByteCodeVerifier.this.disallowedMethodCalls.get(owner);
         if(disallowed != null && disallowed.contains(name)) {
            this.errorDisallowed(owner, name);
         }

         if(!JavaUDFByteCodeVerifier.JAVA_UDF_NAME.equals(owner)) {
            Iterator var7 = JavaUDFByteCodeVerifier.this.disallowedPackages.iterator();

            while(var7.hasNext()) {
               String pkg = (String)var7.next();
               if(owner.startsWith(pkg)) {
                  this.errorDisallowed(owner, name);
               }
            }

            if(!JavaUDFByteCodeVerifier.this.checkMethodCall(owner, name)) {
               this.errorDisallowed(owner, name);
            }
         }

         super.visitMethodInsn(opcode, owner, name, desc, itf);
      }

      private void errorDisallowed(String owner, String name) {
         this.errors.add("call to " + owner.replace('/', '.') + '.' + name + "()");
      }

      public void visitLabel(Label label) {
         super.visitLabel(label);
         Label continueLabel = new Label();
         super.visitMethodInsn(184, "org/apache/cassandra/cql3/functions/JavaUDF", "udfExecCall", "()Z", false);
         super.visitJumpInsn(153, continueLabel);
         if(this.returnType.isPrimitive()) {
            if(this.returnType == Void.TYPE) {
               super.visitInsn(177);
            }

            if(this.returnType == Double.TYPE) {
               super.visitInsn(14);
               super.visitInsn(175);
            } else if(this.returnType == Float.TYPE) {
               super.visitInsn(11);
               super.visitInsn(174);
            } else if(this.returnType == Long.TYPE) {
               super.visitInsn(9);
               super.visitInsn(173);
            } else {
               super.visitInsn(3);
               super.visitInsn(172);
            }
         } else {
            super.visitInsn(1);
            super.visitInsn(176);
         }

         super.visitLabel(continueLabel);
      }

      public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack) {
         super.visitFrame(type, nLocal, local, nStack, stack);
      }

      public void visitInsn(int opcode) {
         switch(opcode) {
         case 194:
         case 195:
            this.errors.add("use of synchronized");
         default:
            super.visitInsn(opcode);
         }
      }
   }
}
