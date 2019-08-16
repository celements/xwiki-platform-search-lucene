package com.celements.search.lucene.index.queue;

import static com.celements.common.MoreObjectsCel.*;

import java.util.Objects;

public class IndexQueuePriority implements Comparable<IndexQueuePriority> {

  public static final IndexQueuePriority LOWEST = new IndexQueuePriority(Integer.MIN_VALUE);
  public static final IndexQueuePriority LOW = new IndexQueuePriority(-1000);
  public static final IndexQueuePriority DEFAULT = new IndexQueuePriority(0);
  public static final IndexQueuePriority HIGH = new IndexQueuePriority(1000);
  public static final IndexQueuePriority HIGHEST = new IndexQueuePriority(Integer.MAX_VALUE);

  public final int value;

  public IndexQueuePriority(int value) {
    this.value = value;
  }

  @Override
  public int compareTo(IndexQueuePriority other) {
    return Integer.compare(this.value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public boolean equals(Object obj) {
    return tryCast(obj, IndexQueuePriority.class)
        .map(other -> this.value == other.value)
        .orElse(false);
  }

  @Override
  public String toString() {
    return "IndexQueuePriority [" + value + "]";
  }

}
