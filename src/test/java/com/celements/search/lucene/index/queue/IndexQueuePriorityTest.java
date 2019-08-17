package com.celements.search.lucene.index.queue;

import static com.celements.search.lucene.index.queue.IndexQueuePriority.*;
import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

public class IndexQueuePriorityTest {

  @Test
  public void test() {
    IndexQueuePriority custom = new IndexQueuePriority(412);
    List<IndexQueuePriority> list = Stream.of(DEFAULT, LOW, LOWEST, HIGH, HIGHEST, custom)
        .collect(toList());
    Collections.sort(list);
    assertSame(HIGHEST, list.remove(0));
    assertSame(HIGH, list.remove(0));
    assertSame(custom, list.remove(0));
    assertSame(DEFAULT, list.remove(0));
    assertSame(LOW, list.remove(0));
    assertSame(LOWEST, list.remove(0));
    assertTrue(list.isEmpty());
  }

}
