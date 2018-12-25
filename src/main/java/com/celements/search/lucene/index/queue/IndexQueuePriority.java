package com.celements.search.lucene.index.queue;

import com.google.common.collect.ImmutableList;

public enum IndexQueuePriority {

  LOWEST, LOWER, LOW, DEFAULT, HIGH, HIGHER, HIGHEST;

  public static int size() {
    return IndexQueuePriority.values().length;
  }

  public static ImmutableList<IndexQueuePriority> list() {
    return ImmutableList.copyOf(IndexQueuePriority.values());
  }

}
