package org.apache.cassandra.utils;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class UnmodifiableArrayList<E> extends ArrayList<E> {
   private static final UnmodifiableArrayList EMPTY_LIST = new UnmodifiableArrayList();
   private boolean frozen;

   private UnmodifiableArrayList(boolean frozen) {
      this.frozen = frozen;
   }

   private UnmodifiableArrayList(boolean frozen, int initialCapacity) {
      super(initialCapacity);
      this.frozen = frozen;
   }

   private UnmodifiableArrayList() {
      this.freeze();
   }

   private UnmodifiableArrayList(Collection<? extends E> all) {
      super(all);
      this.freeze();
   }

   private UnmodifiableArrayList(E... elements) {
      super(elements.length);
      Collections.addAll(this, elements);
      this.freeze();
   }

   private UnmodifiableArrayList(Iterator<? extends E> elements) {
      Objects.requireNonNull(elements);

      while(elements.hasNext()) {
         this.add(elements.next());
      }

      this.freeze();
   }

   private void freeze() {
      this.frozen = true;
   }

   private void unfreeze() {
      this.frozen = false;
   }

   private void unsupportedIfFrozen() {
      if(this.frozen) {
         throw new UnsupportedOperationException();
      }
   }

   public boolean add(E t) {
      this.unsupportedIfFrozen();
      return super.add(t);
   }

   public void add(int index, E element) {
      this.unsupportedIfFrozen();
      super.add(index, element);
   }

   public boolean addAll(Collection<? extends E> c) {
      this.unsupportedIfFrozen();
      return super.addAll(c);
   }

   public boolean addAll(int index, Collection<? extends E> c) {
      this.unsupportedIfFrozen();
      return super.addAll(index, c);
   }

   public E set(int index, E element) {
      this.unsupportedIfFrozen();
      return super.set(index, element);
   }

   public E remove(int index) {
      this.unsupportedIfFrozen();
      return super.remove(index);
   }

   public boolean removeAll(Collection<?> c) {
      this.unsupportedIfFrozen();
      return super.removeAll(c);
   }

   protected void removeRange(int fromIndex, int toIndex) {
      this.unsupportedIfFrozen();
      super.removeRange(fromIndex, toIndex);
   }

   public boolean removeIf(Predicate<? super E> filter) {
      this.unsupportedIfFrozen();
      return super.removeIf(filter);
   }

   public void replaceAll(UnaryOperator<E> operator) {
      this.unsupportedIfFrozen();
      super.replaceAll(operator);
   }

   public boolean retainAll(Collection<?> c) {
      this.unsupportedIfFrozen();
      return super.retainAll(c);
   }

   public boolean remove(Object o) {
      this.unsupportedIfFrozen();
      return super.remove(o);
   }

   public List<E> subList(final int fromIndex, final int toIndex) {
      if(fromIndex < 0) {
         throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
      } else if(toIndex > this.size()) {
         throw new IndexOutOfBoundsException("toIndex = " + toIndex);
      } else if(fromIndex > toIndex) {
         throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
      } else if(toIndex > this.size()) {
         throw new IllegalArgumentException("sublist toIndex:" + toIndex + " should be <= size()");
      } else {
         return (List)(fromIndex == toIndex?emptyList():(fromIndex == 0 && toIndex == this.size()?this:new AbstractList<E>() {
            public E get(int index) {
               return UnmodifiableArrayList.this.get(fromIndex + index);
            }

            public int size() {
               return toIndex - fromIndex;
            }
         }));
      }
   }

   public static <E> UnmodifiableArrayList.Builder<E> builder() {
      return new UnmodifiableArrayList.Builder();
   }

   public static <E> UnmodifiableArrayList.Builder<E> builder(int size) {
      return new UnmodifiableArrayList.Builder(size);
   }

   public static <E> UnmodifiableArrayList<E> emptyList() {
      return EMPTY_LIST;
   }

   public static <E> UnmodifiableArrayList<E> of(E e1) {
      return new UnmodifiableArrayList(new Object[]{e1});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2) {
      return new UnmodifiableArrayList(new Object[]{e1, e2});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4, E e5) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4, e5});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4, E e5, E e6) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4, e5, e6});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4, e5, e6, e7});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7, E e8) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4, e5, e6, e7, e8});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7, E e8, E e9) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4, e5, e6, e7, e8, e9});
   }

   public static <E> UnmodifiableArrayList<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7, E e8, E e9, E e10) {
      return new UnmodifiableArrayList(new Object[]{e1, e2, e3, e4, e5, e6, e7, e8, e9, e10});
   }

   public static <E> UnmodifiableArrayList<E> of(E... elements) {
      switch(elements.length) {
      case 0:
         return EMPTY_LIST;
      default:
         return new UnmodifiableArrayList(elements);
      }
   }

   public static <E> UnmodifiableArrayList<E> copyOf(Collection<E> all) {
      Objects.requireNonNull(all);
      return new UnmodifiableArrayList(all);
   }

   public static <E> UnmodifiableArrayList<E> copyOf(E[] all) {
      Objects.requireNonNull(all);
      return new UnmodifiableArrayList(all);
   }

   public static <E> UnmodifiableArrayList<E> copyOf(Iterable<? extends E> elements) {
      Objects.requireNonNull(elements);
      return elements instanceof Collection?copyOf((Collection)elements):copyOf(elements.iterator());
   }

   public static <E> UnmodifiableArrayList<E> copyOf(Iterator<? extends E> elements) {
      Objects.requireNonNull(elements);
      return new UnmodifiableArrayList(elements);
   }

   public static <E> UnmodifiableArrayList<E> reverseCopyOf(List<E> elements) {
      Objects.requireNonNull(elements);
      UnmodifiableArrayList<E> result = new UnmodifiableArrayList(elements);
      result.unfreeze();
      Collections.reverse(result);
      result.freeze();
      return result;
   }

   public static <F, T> List<T> transformOf(List<F> fromList, Function<? super F, ? extends T> function) {
      UnmodifiableArrayList<T> result = new UnmodifiableArrayList(false, fromList.size());
      Iterator var3 = fromList.iterator();

      while(var3.hasNext()) {
         F f = var3.next();
         T t = function.apply(f);
         result.add(t);
      }

      result.freeze();
      return result;
   }

   Object writeReplace() {
      return Collections.unmodifiableList(new ArrayList(this));
   }

   public static final class Builder<E> {
      private UnmodifiableArrayList<E> contents;
      private boolean built;

      Builder() {
         this.contents = new UnmodifiableArrayList(false, null);
      }

      public Builder(int size) {
         this.contents = new UnmodifiableArrayList(false, size, null);
      }

      public UnmodifiableArrayList.Builder<E> add(E element) {
         this.maybeReplaceAndCopyFrozenContent();
         this.contents.add(element);
         return this;
      }

      private void maybeReplaceAndCopyFrozenContent() {
         if(this.built) {
            UnmodifiableArrayList<E> unfrozenContents = new UnmodifiableArrayList(false, null);
            UnmodifiableArrayList<E> previousContents = this.contents;
            this.contents = unfrozenContents;
            this.built = false;
            this.addAll(previousContents);
         }

      }

      public UnmodifiableArrayList.Builder<E> add(E... elements) {
         this.maybeReplaceAndCopyFrozenContent();
         Collections.addAll(this.contents, elements);
         return this;
      }

      public UnmodifiableArrayList.Builder<E> addAll(Iterable<? extends E> elements) {
         Iterator var2 = elements.iterator();

         while(var2.hasNext()) {
            E element = var2.next();
            this.add(element);
         }

         return this;
      }

      public UnmodifiableArrayList<E> build() {
         this.contents.freeze();
         this.built = true;
         return this.contents;
      }
   }
}
