package org.apache.cassandra.concurrent;

import com.google.common.base.Throwables;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.disposables.EmptyDisposable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrderThreaded;

public class TPCUtils {
   private static final int NUM_CORES = DatabaseDescriptor.getTPCCores();
   private static final OpOrderThreaded.ThreadIdentifier threadIdentifier = new OpOrderThreaded.ThreadIdentifier() {
      public int idFor(Thread t) {
         return TPCUtils.getCoreId(t);
      }

      public boolean barrierPermitted() {
         return !TPCUtils.isTPCThread();
      }
   };

   public TPCUtils() {
   }

   public static boolean isWouldBlockException(Throwable t) {
      return Throwables.getRootCause(t) instanceof TPCUtils.WouldBlockException;
   }

   public static <T> T blockingGet(Single<T> single) {
      if(isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingGet would block TPC thread " + Thread.currentThread().getName());
      } else {
         return single.blockingGet();
      }
   }

   public static void blockingAwait(Completable completable) {
      if(isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingAwait would block TPC thread " + Thread.currentThread().getName());
      } else {
         completable.blockingAwait();
      }
   }

   public static <T> T blockingGet(CompletableFuture<T> future) {
      if(isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingGet would block TPC thread " + Thread.currentThread().getName());
      } else {
         try {
            return future.get();
         } catch (Exception var2) {
            throw org.apache.cassandra.utils.Throwables.cleaned(var2);
         }
      }
   }

   public static void blockingAwait(CompletableFuture future) {
      if(isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingAwait would block TPC thread " + Thread.currentThread().getName());
      } else {
         try {
            future.get();
         } catch (Exception var2) {
            throw org.apache.cassandra.utils.Throwables.cleaned(var2);
         }
      }
   }

   public static <T> CompletableFuture<T> toFuture(Single<T> single) {
      CompletableFuture<T> ret = new CompletableFuture();
      single.subscribe(ret::complete, ret::completeExceptionally);
      return ret;
   }

   public static <T> CompletableFuture<Void> toFutureVoid(Single<T> single) {
      CompletableFuture<Void> ret = new CompletableFuture();
      single.subscribe((result) -> {
         ret.complete(null);
      }, ret::completeExceptionally);
      return ret;
   }

   public static CompletableFuture<Void> toFuture(Completable completable) {
      CompletableFuture<Void> ret = new CompletableFuture();
      completable.subscribe(() -> {
         ret.complete(null);
      }, ret::completeExceptionally);
      return ret;
   }

   public static Completable toCompletable(final CompletableFuture<Void> future) {
      return new Completable() {
         protected void subscribeActual(CompletableObserver observer) {
            observer.onSubscribe(EmptyDisposable.INSTANCE);
            future.whenComplete((res, err) -> {
               if(err == null) {
                  observer.onComplete();
               } else {
                  observer.onError(org.apache.cassandra.utils.Throwables.unwrapped(err));
               }

            });
         }
      };
   }

   public static <T> Single<T> toSingle(final CompletableFuture<T> future) {
      return new Single<T>() {
         protected void subscribeActual(SingleObserver<? super T> observer) {
            observer.onSubscribe(EmptyDisposable.INSTANCE);
            future.whenComplete((res, err) -> {
               if(err == null) {
                  observer.onSuccess(res);
               } else {
                  observer.onError(org.apache.cassandra.utils.Throwables.unwrapped(err));
               }

            });
         }
      };
   }

   public static CompletableFuture<Void> completedFuture() {
      return CompletableFuture.completedFuture(null);
   }

   public static <T> CompletableFuture<T> completedFuture(T value) {
      return CompletableFuture.completedFuture(value);
   }

   public static <T> CompletableFuture<T> completableFuture(Callable<T> callable, ExecutorService executor) {
      assert callable != null : "Received null callable";

      assert executor != null : "Received null executor";

      return CompletableFuture.supplyAsync(() -> {
         try {
            return callable.call();
         } catch (Exception var2) {
            throw new CompletionException(var2);
         }
      }, executor);
   }

   public static <T> CompletableFuture<Void> completableFutureVoid(Callable<T> callable, ExecutorService executor) {
      assert callable != null : "Received null callable";

      assert executor != null : "Received null executor";

      return CompletableFuture.supplyAsync(() -> {
         try {
            callable.call();
            return null;
         } catch (Exception var2) {
            throw new CompletionException(var2);
         }
      }, executor);
   }

   public static int getCoreId() {
      return getCoreId(Thread.currentThread());
   }

   public static int getNumCores() {
      return NUM_CORES;
   }

   public static boolean isOnCore(int coreId) {
      return getCoreId() == coreId;
   }

   public static boolean isOnIO() {
      return isIOThread(Thread.currentThread());
   }

   public static boolean isTPCThread() {
      return isTPCThread(Thread.currentThread());
   }

   public static OpOrder newOpOrder(Object creator) {
      return new OpOrderThreaded(creator, threadIdentifier, NUM_CORES + 1);
   }

   private static int getCoreId(Thread t) {
      return t instanceof TPCThread?((TPCThread)t).coreId():NUM_CORES;
   }

   private static boolean isIOThread(Thread thread) {
      return thread instanceof IOThread;
   }

   private static boolean isTPCThread(Thread thread) {
      return thread instanceof TPCThread;
   }

   public static final class WouldBlockException extends RuntimeException {
      public WouldBlockException(String message) {
         super(message);
      }
   }
}
