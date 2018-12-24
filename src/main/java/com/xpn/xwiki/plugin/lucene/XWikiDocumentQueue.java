/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.xpn.xwiki.plugin.lucene;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Singleton;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUnderflowException;
import org.apache.commons.collections.buffer.UnboundedFifoBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;

import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.LuceneDocId;
import com.celements.search.lucene.index.queue.LuceneIndexingQueue;

/**
 * This class represents a Queue (FirstInFirstOut) for XWikiDocument objects. It is used
 * during indexing of the wiki. The index is updated whenever the processing queue is not
 * empty. This class is threadsafe, as usually several threads add elements and one thread
 * removes them for processing.
 *
 * @version $Id: 04187bfc92c5273f46dd5d519cfd1835df839dd6 $
 */
@Singleton
@ThreadSafe
@Component(XWikiDocumentQueue.NAME)
public class XWikiDocumentQueue implements LuceneIndexingQueue {

  public static final String NAME = "xwiki";

  /** Logging helper object. */
  private static final Logger LOGGER = LoggerFactory.getLogger(XWikiDocumentQueue.class);

  /**
   * Maps names of documents to the document instances.
   */
  private final Map<LuceneDocId, IndexData> documentsByName = new HashMap<>();

  /**
   * Maintains FIFO order.
   */
  private final Buffer namesQueue = new UnboundedFifoBuffer();

  /**
   * Remove an item from the queue and return it. Since this is a FIFO, the element
   * returned will be the oldes one in the queue.
   *
   * @return The oldest element in the queue.
   * @throws BufferUnderflowException
   *           If the queue is empty.
   */
  @Override
  public synchronized IndexData remove() throws NoSuchElementException {
    LOGGER.debug("removing element from queue.");
    try {
      return this.documentsByName.remove(this.namesQueue.remove());
    } catch (BufferUnderflowException exc) {
      throw new NoSuchElementException(exc.getMessage());
    }
  }

  @Override
  public synchronized boolean contains(LuceneDocId id) {
    return this.documentsByName.containsKey(id);
  }

  /**
   * Adds an item to the queue. Since this is a FIFO, it will be removed after all the
   * other items already in the queue have been processed. If the element was already in
   * the queue, it will not be added again (as a duplicate), its position will remain
   * unchanged, but the associated data is updated, so the new version will replace the
   * old unprocessed one.
   *
   * @param data
   *          IndexData object to add to the queue.
   */
  @Override
  @SuppressWarnings("unchecked")
  public synchronized void add(IndexData data) {
    LuceneDocId key = data.getId();

    LOGGER.debug("adding element to queue. Key: " + key);
    if (!this.documentsByName.containsKey(key)) {
      // Document with this name not yet in the Queue, so add it
      this.namesQueue.add(key);
    }

    // In any case put new version of this document in the map, overwriting
    // possibly existing older version
    this.documentsByName.put(key, data);
  }

  /**
   * Check if the queue is empty or not.
   *
   * @return <code>true</code> if the queue is empty, <code>false</code> otherwise.
   */
  @Override
  public synchronized boolean isEmpty() {
    return this.namesQueue.isEmpty();
  }

  /**
   * Returns the number of elements in the queue.
   *
   * @return Number of elements in the queue.
   */
  @Override
  public synchronized int getSize() {
    return this.namesQueue.size();
  }

  @Override
  public IndexData take() throws InterruptedException {
    throw new UnsupportedOperationException("non blocking queue");
  }
}
