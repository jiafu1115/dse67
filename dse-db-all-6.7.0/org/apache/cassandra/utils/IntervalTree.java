package org.apache.cassandra.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntervalTree<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements Iterable<I> {
    private static final Logger logger = LoggerFactory.getLogger(IntervalTree.class);
    private static final IntervalTree EMPTY_TREE = new IntervalTree((Collection) null);
    private final IntervalTree<C, D, I>.IntervalNode head;
    private final int count;

    protected IntervalTree(Collection<I> intervals) {
        this.head = intervals != null && !intervals.isEmpty() ? new IntervalTree.IntervalNode(intervals) : null;
        this.count = intervals == null ? 0 : intervals.size();
    }

    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals) {
        return intervals != null && !intervals.isEmpty() ? new IntervalTree(intervals) : emptyTree();
    }

    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree() {
        return EMPTY_TREE;
    }

    public int intervalCount() {
        return this.count;
    }

    public boolean isEmpty() {
        return this.head == null;
    }

    public C max() {
        if (this.head == null) {
            throw new IllegalStateException();
        } else {
            return this.head.high;
        }
    }

    public C min() {
        if (this.head == null) {
            throw new IllegalStateException();
        } else {
            return this.head.low;
        }
    }

    public List<D> search(Interval<C, D> searchInterval) {
        if (this.head == null) {
            return UnmodifiableArrayList.emptyList();
        } else {
            List<D> results = new ArrayList();
            this.head.searchInternal(searchInterval, results);
            return results;
        }
    }

    public List<D> search(C point) {
        return this.search(Interval.create(point, point, null));
    }

    public Iterator<I> iterator() {
        return (Iterator) (this.head == null ? Collections.emptyIterator() : new IntervalTree.TreeIterator(this.head));
    }

    public String toString() {
        return "<" + Joiner.on(", ").join(this) + ">";
    }

    public boolean equals(Object o) {
        if (!(o instanceof IntervalTree)) {
            return false;
        } else {
            IntervalTree that = (IntervalTree) o;
            return Iterators.elementsEqual(this.iterator(), that.iterator());
        }
    }

    public final int hashCode() {
        int result = 0;

        Interval interval;
        for (Iterator var2 = this.iterator(); var2.hasNext(); result = 31 * result + interval.hashCode()) {
            interval = (Interval) var2.next();
        }

        return result;
    }

    private class TreeIterator extends AbstractIterator<I> {
        private final Deque<IntervalTree<C, D, I>.IntervalNode> stack = new ArrayDeque();
        private Iterator<I> current;

        TreeIterator(IntervalTree<C, D, I>.IntervalNode node) {
            this.gotoMinOf(node);
        }

        protected I computeNext() {
            while (this.current == null || !this.current.hasNext()) {
                IntervalTree<C, D, I>.IntervalNode node = (IntervalTree.IntervalNode) this.stack.pollFirst();
                if (node == null) {
                    return (I) this.endOfData();
                }

                this.current = node.intersectsLeft.iterator();
                this.gotoMinOf(node.right);
            }

            return (I) this.current.next();
        }

        private void gotoMinOf(IntervalTree<C, D, I>.IntervalNode node) {
            while (node != null) {
                this.stack.offerFirst(node);
                node = node.left;
            }

        }
    }

    private class IntervalNode {
        final C center;
        final C low;
        final C high;
        final List<I> intersectsLeft;
        final List<I> intersectsRight;
        final IntervalTree<C, D, I>.IntervalNode left;
        final IntervalTree<C, D, I>.IntervalNode right;

        public IntervalNode(Collection<I> toBisect) {
            assert !toBisect.isEmpty();

            IntervalTree.logger.trace("Creating IntervalNode from {}", toBisect);
            if (toBisect.size() == 1) {
                I interval = (I) toBisect.iterator().next();
                this.low = (C) interval.min;
                this.center = (C) interval.max;
                this.high = (C) interval.max;
                List<I> l = UnmodifiableArrayList.of(interval);
                this.intersectsLeft = l;
                this.intersectsRight = l;
                this.left = null;
                this.right = null;
            } else {
                List<C> allEndpoints = new ArrayList(toBisect.size() * 2);
                Iterator var10 = toBisect.iterator();

                for (I intervalx : toBisect) {
                    allEndpoints.add(intervalx.min);
                    allEndpoints.add(intervalx.max);
                }

                Collections.sort(allEndpoints);
                this.low = (C) allEndpoints.get(0);
                this.center = (C) allEndpoints.get(toBisect.size());
                this.high = (C) allEndpoints.get(allEndpoints.size() - 1);
                List<I> intersects = new ArrayList();
                List<I> leftSegment = new ArrayList();
                List<I> rightSegment = new ArrayList();

                for (I candidate : toBisect) {
                    if (((Comparable) candidate.max).compareTo(this.center) < 0) {
                        leftSegment.add(candidate);
                    } else if (((Comparable) candidate.min).compareTo(this.center) > 0) {
                        rightSegment.add(candidate);
                    } else {
                        intersects.add(candidate);
                    }
                }

                this.intersectsLeft = (List<I>) Interval.minOrdering().sortedCopy((Iterable) intersects);
                this.intersectsRight = (List<I>) Interval.maxOrdering().sortedCopy((Iterable) intersects);
                this.left = leftSegment.isEmpty() ? null : IntervalTree.this.new IntervalNode(leftSegment);
                this.right = rightSegment.isEmpty() ? null : IntervalTree.this.new IntervalNode(rightSegment);

                assert intersects.size() + leftSegment.size() + rightSegment.size() == toBisect.size() : "intersects (" + String.valueOf(intersects.size()) + ") + leftSegment (" + leftSegment.size() + ") + rightSegment (" + rightSegment.size() + ") != toBisect (" + toBisect.size() + ")";
            }

        }

        void searchInternal(Interval<C, D> searchInterval, List<D> results) {
            int j;
            if (this.center.compareTo(searchInterval.min) < 0) {
                j = Interval.<C, D>maxOrdering().binarySearchAsymmetric(this.intersectsRight, searchInterval.min, AsymmetricOrdering.Op.CEIL);
                if (j == this.intersectsRight.size() && this.high.compareTo(searchInterval.min) < 0) {
                    return;
                }

                while (j < this.intersectsRight.size()) {
                    results.add((D) this.intersectsRight.get(j++).data);
                }

                if (this.right != null) {
                    this.right.searchInternal(searchInterval, results);
                }
            } else if (this.center.compareTo(searchInterval.max) > 0) {
                j = Interval.<C, D>minOrdering().binarySearchAsymmetric(this.intersectsLeft, searchInterval.max, AsymmetricOrdering.Op.HIGHER);
                if (j == 0 && this.low.compareTo(searchInterval.max) > 0) {
                    return;
                }

                for (int i = 0; i < j; ++i) {
                    results.add((D) this.intersectsLeft.get(i).data);
                }

                if (this.left != null) {
                    this.left.searchInternal(searchInterval, results);
                }
            } else {
                Iterator var5 = this.intersectsLeft.iterator();

                for (Interval<C, D> interval : this.intersectsLeft) {
                    results.add(interval.data);
                }

                if (this.left != null) {
                    this.left.searchInternal(searchInterval, results);
                }

                if (this.right != null) {
                    this.right.searchInternal(searchInterval, results);
                }
            }

        }
    }
}
