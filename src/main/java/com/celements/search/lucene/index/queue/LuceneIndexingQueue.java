package com.celements.search.lucene.index.queue;

import java.util.NoSuchElementException;

import org.xwiki.component.annotation.ComponentRole;

import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.LuceneDocId;

@ComponentRole
public interface LuceneIndexingQueue {

  int getSize();

  boolean isEmpty();

  /**
   * check if the queue contains data for the given id
   */
  boolean contains(LuceneDocId id);

  /**
   * Adds an element to the queue. If the element was already in the queue, the associated data is
   * updated but its position will remain unchanged.
   */
  void add(IndexData data);

  /**
   * Retrieves and removes the head of the queue.
   *
   * @throws NoSuchElementException
   *           if the queue is empty
   */
  IndexData remove() throws NoSuchElementException;

}