package com.xpn.xwiki.plugin.lucene.index.queue;

import java.util.NoSuchElementException;

import org.xwiki.component.annotation.ComponentRole;

import com.xpn.xwiki.plugin.lucene.index.IndexData;

@ComponentRole
public interface LuceneIndexingQueue {

  boolean contains(String id);

  void add(IndexData data);

  IndexData remove() throws NoSuchElementException;

  IndexData take() throws InterruptedException;

  boolean isEmpty();

  int getSize();

}
