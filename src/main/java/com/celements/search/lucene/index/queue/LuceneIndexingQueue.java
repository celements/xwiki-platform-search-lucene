package com.celements.search.lucene.index.queue;

import java.util.NoSuchElementException;

import org.xwiki.component.annotation.ComponentRole;

import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.LuceneDocId;

@ComponentRole
public interface LuceneIndexingQueue {

  boolean contains(LuceneDocId id);

  void add(IndexData data);

  IndexData remove() throws NoSuchElementException;

  IndexData take() throws InterruptedException, UnsupportedOperationException;

  boolean isEmpty();

  int getSize();

}
