package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.function.Supplier;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.LightweightRecycler;

public abstract class IncrementalTrieWriterBase<Value, Dest, Node extends IncrementalTrieWriterBase.BaseNode<Value, Node>> implements IncrementalTrieWriter<Value> {
   protected final Deque<Node> stack = new ArrayDeque();
   protected final TrieSerializer<Value, ? super Dest> serializer;
   protected final Dest dest;
   protected ByteSource prev = null;
   long count = 0L;

   public IncrementalTrieWriterBase(TrieSerializer<Value, ? super Dest> serializer, Dest dest, Node root) {
      this.serializer = serializer;
      this.dest = dest;
      this.stack.addLast(root);
   }

   protected void reset(Node root) {
      this.prev = null;
      this.count = 0L;
      this.stack.clear();
      this.stack.addLast(root);
   }

   public void close() {
      this.prev = null;
      this.count = 0L;
      this.stack.clear();
   }

   public void add(ByteSource next, Value value) throws IOException {
      ++this.count;
      int stackpos = 0;
      next.reset();
      int n = next.next();
      if(this.prev != null) {
         this.prev.reset();

         int p;
         for(p = this.prev.next(); n == p; p = this.prev.next()) {
            assert n != -1 : String.format("Incremental trie requires unique sorted keys, got equal %s after %s.", new Object[]{next, this.prev});

            ++stackpos;
            n = next.next();
         }

         assert p < n : String.format("Incremental trie requires sorted keys, got %s after %s.", new Object[]{next, this.prev});
      }

      this.prev = next;

      while(this.stack.size() > stackpos + 1) {
         this.completeLast();
      }

      IncrementalTrieWriterBase.BaseNode node;
      for(node = (IncrementalTrieWriterBase.BaseNode)this.stack.getLast(); n != -1; n = next.next()) {
         this.stack.addLast(node = node.addChild((byte)n));
         ++stackpos;
      }

      Value existingPayload = node.setPayload(value);

      assert existingPayload == null;

   }

   public long complete() throws IOException {
      Node root = (IncrementalTrieWriterBase.BaseNode)this.stack.getFirst();
      return root.filePos != -1L?root.filePos:this.performCompletion().filePos;
   }

   Node performCompletion() throws IOException {
      IncrementalTrieWriterBase.BaseNode root;
      for(root = null; !this.stack.isEmpty(); root = this.completeLast()) {
         ;
      }

      this.stack.addLast(root);
      return root;
   }

   public long count() {
      return this.count;
   }

   protected Node completeLast() throws IOException {
      Node node = (IncrementalTrieWriterBase.BaseNode)this.stack.removeLast();
      this.complete(node);
      return node;
   }

   abstract void complete(Node var1) throws IOException;

   public abstract IncrementalTrieWriter.PartialTail makePartialRoot() throws IOException;

   abstract static class BaseNode<Value, Node extends IncrementalTrieWriterBase.BaseNode<Value, Node>> implements SerializationNode<Value> {
      private static final int CHILDREN_LIST_RECYCLER_LIMIT = 1024;
      private static final LightweightRecycler<ArrayList> CHILDREN_LIST_RECYCLER = new LightweightRecycler(1024);
      private static final ArrayList EMPTY_LIST = new ArrayList(0);
      Value payload;
      ArrayList<Node> children;
      final int transition;
      long filePos = -1L;

      private static <Node> ArrayList<Node> allocateChildrenList() {
         return (ArrayList)CHILDREN_LIST_RECYCLER.reuseOrAllocate(() -> {
            return new ArrayList(4);
         });
      }

      private static <Node> void recycleChildrenList(ArrayList<Node> children) {
         CHILDREN_LIST_RECYCLER.tryRecycle(children);
      }

      BaseNode(int transition) {
         this.children = EMPTY_LIST;
         this.transition = transition;
      }

      public Value payload() {
         return this.payload;
      }

      public Value setPayload(Value newPayload) {
         Value p = this.payload;
         this.payload = newPayload;
         return p;
      }

      public Node addChild(byte b) {
         assert this.children.isEmpty() || (((IncrementalTrieWriterBase.BaseNode)this.children.get(this.children.size() - 1)).transition & 255) < (b & 255);

         Node node = this.newNode(b);
         if(this.children == EMPTY_LIST) {
            this.children = allocateChildrenList();
         }

         this.children.add(node);
         return node;
      }

      public int childCount() {
         return this.children.size();
      }

      void finalizeWithPosition(long position) {
         this.filePos = position;
         if(this.children != EMPTY_LIST) {
            recycleChildrenList(this.children);
         }

         this.children = null;
         this.payload = null;
      }

      public int transition(int i) {
         return ((IncrementalTrieWriterBase.BaseNode)this.children.get(i)).transition;
      }

      public String toString() {
         return String.format("%02x", new Object[]{Integer.valueOf(this.transition)});
      }

      abstract Node newNode(byte var1);
   }

   static class PTail implements IncrementalTrieWriter.PartialTail {
      long root;
      long cutoff;
      long count;
      ByteBuffer tail;

      PTail() {
      }

      public long root() {
         return this.root;
      }

      public long cutoff() {
         return this.cutoff;
      }

      public ByteBuffer tail() {
         return this.tail;
      }

      public long count() {
         return this.count;
      }
   }
}
