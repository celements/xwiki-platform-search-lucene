package com.xpn.xwiki.plugin.lucene.index.queue;

import static com.google.common.base.MoreObjects.*;
import static com.google.common.base.Preconditions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Singleton;

import org.xwiki.component.annotation.Component;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.xpn.xwiki.plugin.lucene.index.IndexData;

/**
 * This class represents a queue for lucene data to be indexed. It's elements are ordered by their
 * respective {@link IndexQueuePriority}. For equal priority the queue behaves as FIFO.
 */
@ThreadSafe
@Singleton
@Component
public class LuceneIndexingPriorityQueue implements LuceneIndexingQueue {

  private final AtomicLong SEQUENCE_COUNTER = new AtomicLong();

  private final PriorityQueue<IndexQueueElement> queue = new PriorityQueue<>();
  private final Map<String, IndexData> dataMap = new HashMap<>();

  @Override
  public synchronized int getSize() {
    return queue.size();
  }

  @Override
  public synchronized boolean isEmpty() {
    return queue.isEmpty();
  }

  /**
   * Time complexity O(1)
   *
   * @param id
   * @return true if the queue contains the given id
   */
  @Override
  public synchronized boolean contains(String id) {
    return dataMap.containsKey(id);
  }

  /**
   * Adds an element to the queue. If the element was already in the queue,the associated data is
   * updated but its position will remain unchanged.
   * Time complexity O(log(n))
   *
   * @param data
   *          IndexData data item to add to the queue.
   */
  @Override
  public synchronized void add(IndexData data) {
    if (dataMap.put(data.getId(), data) == null) {
      queue.add(new IndexQueueElement(data.getId(), data.getPriority()));
    }
  }

  /**
   * Time complexity O(log(n))
   *
   * @return the oldest element in the queue with the highest priority
   * @throws NoSuchElementException
   *           if the queue is empty
   */
  @Override
  public synchronized IndexData remove() throws NoSuchElementException {
    return dataMap.remove(queue.remove().id);
  }

  private class IndexQueueElement implements Comparable<IndexQueueElement> {

    final String id;
    final IndexQueuePriority priority;
    final long sequence;

    IndexQueueElement(String id, IndexQueuePriority priority) {
      this.id = checkNotNull(Strings.emptyToNull(id));
      this.priority = firstNonNull(priority, IndexQueuePriority.DEFAULT);
      this.sequence = SEQUENCE_COUNTER.incrementAndGet();
    }

    @Override
    public int compareTo(IndexQueueElement other) {
      ComparisonChain cmp = ComparisonChain.start();
      cmp = cmp.compare(this.priority, other.priority, Ordering.natural());
      cmp = cmp.compare(this.sequence, other.sequence, Ordering.natural().reverse());
      return cmp.result();
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IndexQueueElement) {
        return Objects.equals(this.id, ((IndexQueueElement) obj).id);
      }
      return false;
    }

    @Override
    public String toString() {
      return "IndexQueueElement [id=" + id + ", priority=" + priority + ", sequence=" + sequence
          + "]";
    }

  }

}
