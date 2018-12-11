package com.celements.search.lucene.index.queue;

import java.util.NoSuchElementException;

import org.xwiki.component.annotation.ComponentRole;

import com.celements.search.lucene.index.IndexData;

@ComponentRole
public interface LuceneIndexingQueue {

  boolean contains(String id);

  void add(IndexData data);

  IndexData remove() throws NoSuchElementException;

  IndexData take() throws InterruptedException, UnsupportedOperationException;

  boolean isEmpty();

  int getSize();

}
