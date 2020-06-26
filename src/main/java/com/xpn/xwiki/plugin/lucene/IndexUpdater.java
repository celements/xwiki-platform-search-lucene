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

import static com.google.common.collect.ImmutableMap.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.observation.ObservationManager;

import com.celements.common.observation.event.AbstractEntityEvent;
import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.model.context.ModelContext;
import com.celements.model.util.ModelUtils;
import com.celements.model.util.References;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.plugin.lucene.indexExtension.ILuceneIndexExtensionServiceRole;
import com.xpn.xwiki.plugin.lucene.observation.event.LuceneDocumentDeletedEvent;
import com.xpn.xwiki.plugin.lucene.observation.event.LuceneDocumentDeletingEvent;
import com.xpn.xwiki.plugin.lucene.observation.event.LuceneDocumentIndexedEvent;
import com.xpn.xwiki.plugin.lucene.observation.event.LuceneDocumentIndexingEvent;
import com.xpn.xwiki.util.AbstractXWikiRunnable;
import com.xpn.xwiki.web.Utils;

/**
 * @version $Id: ced4ee86b2d2cf5830598a4a3aefcea8394d60e6 $
 */
public class IndexUpdater extends AbstractXWikiRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexUpdater.class);

  static final String PROP_INDEXING_INTERVAL = "xwiki.plugins.lucene.indexinterval";

  static final String PROP_COMMIT_INTERVAL = "xwiki.plugins.lucene.commitinterval";

  /**
   * The maximum number of milliseconds we have to wait before this thread is safely
   * closed.
   */
  private static final long EXIT_INTERVAL = 3000;

  /**
   * Collecting all the fields for using up in search
   */
  private static final Set<String> COLLECTED_FIELDS = Collections.newSetFromMap(
      new ConcurrentHashMap<String, Boolean>());

  final LucenePlugin plugin;

  final IndexWriter writer;

  /**
   * Milliseconds of sleep between checks for changed documents.
   */
  private final long indexingInterval;

  private final long commitInterval;

  private final ImmutableMap<IndexQueuePriority, XWikiDocumentQueue> queues = Stream
      .of(IndexQueuePriority.values())
      .sorted(Ordering.natural().reversed())
      .collect(toImmutableMap(prio -> prio, prio -> new XWikiDocumentQueue()));

  private final AtomicBoolean exit = new AtomicBoolean(false);

  private final AtomicBoolean optimize = new AtomicBoolean(false);

  IndexUpdater(IndexWriter writer, LucenePlugin plugin, XWikiContext context) throws IOException {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, context.clone());
    this.plugin = plugin;
    this.indexingInterval = 1000 * context.getWiki().ParamAsLong(PROP_INDEXING_INTERVAL, 30);
    this.commitInterval = context.getWiki().ParamAsLong(PROP_COMMIT_INTERVAL, 5000);
    this.writer = writer;
  }

  public boolean isExit() {
    return exit.get();
  }

  /**
   * if exit is being set, the IndexUpdater will no longer accept new queue entries, finishes
   * processing the queue and then shut down gracefully
   */
  public void doExit() {
    LOGGER.info("doExit called");
    exit.set(true);
  }

  public void doOptimize() {
    optimize.set(true);
  }

  /**
   * Return a reference to the directory that this updater is currently working with.
   */
  public Directory getDirectory() {
    return writer.getDirectory();
  }

  /**
   * Main loop. Polls the queue for documents to be indexed.
   *
   * @see java.lang.Runnable#run()
   */
  @Override
  protected void runInternal() {
    LOGGER.info("IndexUpdater started");
    getContext().setWikiRef(getModelUtils().getMainWikiRef());
    try {
      runMainLoop();
    } catch (Throwable exc) {
      LOGGER.error("Unexpected error occured", exc);
      throw exc;
    }
    LOGGER.info("IndexUpdater finished");
  }

  /**
   * Main loop. Polls the queue for documents to be indexed.
   */
  private void runMainLoop() {
    long indexingTimer = 0;
    while (!isExit()) {
      try {
        // Check if the indexing interval elapsed.
        if (indexingTimer == 0) {
          // Reset the indexing timer.
          indexingTimer = this.indexingInterval;
          pollIndexQueue(); // Poll the queue for documents to be indexed
          optimizeIndex(); // optimize index if requested
        }
        // Remove the exit interval from the indexing timer.
        long sleepInterval = Math.min(EXIT_INTERVAL, indexingTimer);
        indexingTimer -= sleepInterval;
        Thread.sleep(sleepInterval);
      } catch (IOException | InterruptedException exc) {
        LOGGER.error("failed to update index", exc);
        doExit();
      }
    }
  }

  private void optimizeIndex() throws IOException {
    if (optimize.compareAndSet(true, false)) {
      LOGGER.warn("started optimizing lucene index");
      writer.optimize();
      LOGGER.warn("finished optimizing lucene index");
    }
  }

  /**
   * Polls the queue for documents to be indexed.
   *
   * @throws IOException
   */
  private void pollIndexQueue() throws IOException {
    if (queues().anyMatch(q -> !q.isEmpty())) {
      try {
        updateIndex();
      } finally {
        getContext().setWikiRef(getModelUtils().getMainWikiRef());
      }
    } else {
      LOGGER.debug("pollIndexQueue: queue empty, nothing to do");
    }
  }

  private void updateIndex() throws IOException {
    LOGGER.debug("updateIndex started");
    boolean hasUncommitedWrites = false;
    long lastCommitTime = System.currentTimeMillis();
    Optional<AbstractIndexData> next;
    do {
      next = queues().filter(q -> !q.isEmpty()).findFirst().map(XWikiDocumentQueue::remove);
      next.ifPresent(this::indexData);
      hasUncommitedWrites |= next.isPresent();
      if (((System.currentTimeMillis() - lastCommitTime) >= commitInterval)) {
        commitIndex();
        lastCommitTime = System.currentTimeMillis();
        hasUncommitedWrites = false;
      }
      checkForInterrupt();
    } while (next.isPresent());
    if (hasUncommitedWrites) {
      commitIndex();
    }
    LOGGER.debug("updateIndex finished");
  }

  /**
   * should be called regularly to check for interrupt flag and set exit
   */
  private void checkForInterrupt() {
    if (Thread.currentThread().isInterrupted()) {
      LOGGER.error("IndexUpdater was interrupted, shutting down");
      doExit();
    }
  }

  private void indexData(AbstractIndexData data) {
    try {
      LOGGER.trace("indexData: start [{}]", data.getEntityReference());
      getContext().setWikiRef(References.extractRef(data.getEntityReference(),
          WikiReference.class).or(getContext().getWikiRef()));
      if (data.isDeleted()) {
        removeFromIndex(data);
      } else {
        try {
          addToIndex(data);
        } catch (DocumentNotExistsException dne) {
          LOGGER.info("indexData: removing inexistent [{}]", data.getEntityReference(), dne);
          removeFromIndex(data);
        }
      }
      LOGGER.trace("indexData: finished [{}]", data.getEntityReference());
    } catch (Exception exc) {
      LOGGER.warn("indexData: error [{}], {}: {}", data, exc.getClass(), exc.getMessage(), exc);
    }
  }

  private void addToIndex(AbstractIndexData data) throws IOException, DocumentNotExistsException {
    LOGGER.debug("addToIndex: '{}'", data);
    EntityReference ref = data.getEntityReference();
    notify(data, new LuceneDocumentIndexingEvent(ref));
    Document luceneDoc = new Document();
    data.addDataToLuceneDocument(luceneDoc);
    getLuceneExtensionService().extend(data, luceneDoc);
    collectFields(luceneDoc);
    writer.updateDocument(data.getTerm(), luceneDoc);
    notify(data, new LuceneDocumentIndexedEvent(ref));
    LOGGER.trace("addToIndex: [{}] - {}", data.getTerm(), luceneDoc);
  }

  // collecting all the fields for using up in search
  // FIXME (Marc Sladek) this doesn't work after restarts as long as there was no doc indexed with
  // the required fields, move to database instead of ram? or is there another solution to the
  // problem it tries to solve?
  private void collectFields(Document luceneDoc) {
    for (Fieldable field : luceneDoc.getFields()) {
      COLLECTED_FIELDS.add(field.name());
    }
  }

  private void removeFromIndex(AbstractIndexData data) throws IOException {
    LOGGER.debug("removeFromIndex: '{}'", data);
    EntityReference ref = data.getEntityReference();
    if (ref != null) {
      notify(data, new LuceneDocumentDeletingEvent(ref));
    }
    writer.deleteDocuments(data.getTerm());
    if (ref != null) {
      notify(data, new LuceneDocumentDeletedEvent(ref));
    }
  }

  public void commitIndex() throws IOException {
    LOGGER.debug("commitIndex");
    writer.commit();
    plugin.closeSearcherProvider();
  }

  public void queue(AbstractIndexData data) {
    if (!isExit()) {
      LOGGER.debug("queue{}: '{}'", (data.isDeleted() ? " delete" : ""), data.getId());
      queues.get(data.getPriority()).add(data);
    } else {
      throw new IllegalStateException("IndexUpdater has been shut down");
    }
  }

  /**
   * @return the number of documents in all queues.
   */
  public long getQueueSize() {
    return queues().mapToInt(XWikiDocumentQueue::getSize).sum();
  }

  /**
   * @return the number of documents in the queue.
   */
  public long getQueueSize(@NotNull IndexQueuePriority priority) {
    return queues.get(priority).getSize();
  }

  /**
   * @return the number of documents in Lucene index writer.
   */
  // TODO why is writer used for this?
  public long getLuceneDocCount() {
    int n = -1;
    try {
      n = writer.numDocs();
    } catch (IOException e) {
      LOGGER.error("Failed to get the number of documents in Lucene index writer", e);
    }
    return n;
  }

  public Set<String> getCollectedFields() {
    return new HashSet<>(COLLECTED_FIELDS);
  }

  private Stream<XWikiDocumentQueue> queues() {
    return queues.values().stream();
  }

  private void notify(AbstractIndexData data, AbstractEntityEvent event) {
    if (data.notifyObservationEvents()) {
      Utils.getComponent(ObservationManager.class).notify(event, event.getReference(),
          getContext().getXWikiContext());
    } else {
      LOGGER.debug("skip notify '{}' for '{}'", event, data);
    }
  }

  private ILuceneIndexExtensionServiceRole getLuceneExtensionService() {
    return Utils.getComponent(ILuceneIndexExtensionServiceRole.class);
  }

  private ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

  private ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

}
