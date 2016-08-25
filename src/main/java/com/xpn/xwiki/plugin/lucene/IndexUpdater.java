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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.bridge.event.DocumentCreatedEvent;
import org.xwiki.bridge.event.DocumentDeletedEvent;
import org.xwiki.bridge.event.DocumentUpdatedEvent;
import org.xwiki.bridge.event.WikiDeletedEvent;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.observation.EventListener;
import org.xwiki.observation.ObservationManager;
import org.xwiki.observation.event.Event;

import com.celements.common.observation.event.AbstractEntityEvent;
import com.celements.model.context.ModelContext;
import com.celements.model.util.ModelUtils;
import com.celements.model.util.References;
import com.google.common.base.Preconditions;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.internal.event.AbstractAttachmentEvent;
import com.xpn.xwiki.internal.event.AttachmentAddedEvent;
import com.xpn.xwiki.internal.event.AttachmentDeletedEvent;
import com.xpn.xwiki.internal.event.AttachmentUpdatedEvent;
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
public class IndexUpdater extends AbstractXWikiRunnable implements EventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexUpdater.class);

  static final String PROP_INDEXING_INTERVAL = "xwiki.plugins.lucene.indexinterval";

  static final String PROP_COMMIT_INTERVAL = "xwiki.plugins.lucene.commitinterval";

  public static final String NAME = "lucene";

  /**
   * The maximum number of milliseconds we have to wait before this thread is safely
   * closed.
   */
  private static final long EXIT_INTERVAL = 3000;

  private static final List<Event> EVENTS = Arrays.<Event>asList(new DocumentUpdatedEvent(),
      new DocumentCreatedEvent(), new DocumentDeletedEvent(), new AttachmentAddedEvent(),
      new AttachmentDeletedEvent(), new AttachmentUpdatedEvent());

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

  private final XWikiDocumentQueue queue = new XWikiDocumentQueue();

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
    getContext().setWikiRef(getContext().getMainWikiRef());
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
    if (queue.isEmpty()) {
      LOGGER.debug("pollIndexQueue: queue empty, nothing to do");
    } else {
      try {
        updateIndex();
      } finally {
        getContext().setWikiRef(getContext().getMainWikiRef());
      }
    }
  }

  private void updateIndex() throws IOException {
    LOGGER.info("updateIndex started");
    boolean hasUncommitedWrites = false;
    long lastCommitTime = System.currentTimeMillis();
    while (!queue.isEmpty()) {
      AbstractIndexData data = queue.remove();
      try {
        LOGGER.debug("updateIndex start document '{}'", data.getEntityReference());
        indexData(data);
        hasUncommitedWrites = true;
        LOGGER.debug("updateIndex successfully finished document '{}'", data.getEntityReference());
      } catch (Exception exc) {
        LOGGER.error("error indexing document '{}'", data.getEntityReference(), exc);
      }
      if ((System.currentTimeMillis() - lastCommitTime) >= commitInterval) {
        commitIndex();
        lastCommitTime = System.currentTimeMillis();
        hasUncommitedWrites = false;
      }
      checkForInterrupt();
    }
    if (hasUncommitedWrites) {
      commitIndex();
    }
    LOGGER.info("updateIndex finished");
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

  private void indexData(AbstractIndexData data) throws IOException, XWikiException {
    getContext().setWikiRef(References.extractRef(data.getEntityReference(),
        WikiReference.class).or(getContext().getWikiRef()));
    if (data.isDeleted()) {
      removeFromIndex(data);
    } else {
      addToIndex(data);
    }
  }

  private void addToIndex(AbstractIndexData data) throws IOException, XWikiException {
    LOGGER.debug("addToIndex: '{}'", data);
    EntityReference ref = data.getEntityReference();
    notify(new LuceneDocumentIndexingEvent(ref));
    Document luceneDoc = new Document();
    data.addDataToLuceneDocument(luceneDoc, getContext().getXWikiContext());
    getLuceneExtensionService().extend(data, luceneDoc);
    collectFields(luceneDoc);
    writer.updateDocument(data.getTerm(), luceneDoc);
    notify(new LuceneDocumentIndexedEvent(ref));
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

  private void removeFromIndex(AbstractIndexData data) throws CorruptIndexException, IOException {
    LOGGER.debug("removeFromIndex: '{}'", data);
    EntityReference ref = data.getEntityReference();
    if (ref != null) {
      notify(new LuceneDocumentDeletingEvent(ref));
    }
    writer.deleteDocuments(data.getTerm());
    if (ref != null) {
      notify(new LuceneDocumentDeletedEvent(ref));
    }
  }

  public void commitIndex() throws IOException {
    LOGGER.debug("commitIndex");
    writer.commit();
    plugin.openSearchers();
  }

  public void queueDeletion(String docId) {
    LOGGER.debug("queueDeletion: '{}'", docId);
    Preconditions.checkNotNull(docId);
    queue(new DeleteData(docId));
  }

  public void queueDocument(XWikiDocument document, boolean deleted) {
    LOGGER.debug("queueDocument: '{}'", document);
    Preconditions.checkNotNull(document);
    queue(new DocumentData(document, deleted, getContext().getXWikiContext()));
  }

  public void queueAttachment(XWikiAttachment attachment, boolean deleted) {
    LOGGER.debug("queueAttachment: '{}'", attachment);
    Preconditions.checkNotNull(attachment);
    queue(new AttachmentData(attachment, deleted, getContext().getXWikiContext()));
  }

  public void queueAttachment(XWikiDocument document, String attachmentName, boolean deleted) {
    LOGGER.debug("queueAttachment: '{}', '{}'", document, attachmentName);
    Preconditions.checkNotNull(document);
    Preconditions.checkNotNull(attachmentName);
    queue(new AttachmentData(document, attachmentName, deleted, getContext().getXWikiContext()));
  }

  public int queueAttachments(XWikiDocument document) {
    int ret = 0;
    LOGGER.debug("queueAttachments: '{}'", document);
    Preconditions.checkNotNull(document);
    for (XWikiAttachment attachment : document.getAttachmentList()) {
      queueAttachment(attachment, false);
      ret++;
    }
    return ret;
  }

  public void queueWiki(WikiReference wikiRef, boolean deleted) {
    LOGGER.debug("queueWiki: '{}'", wikiRef);
    Preconditions.checkNotNull(wikiRef);
    queue(new WikiData(wikiRef, deleted));
  }

  private void queue(AbstractIndexData data) {
    if (!isExit()) {
      queue.add(data);
    } else {
      throw new IllegalStateException("IndexUpdater has been shut down");
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  // @Override
  @Override
  public List<Event> getEvents() {
    return EVENTS;
  }

  @Override
  public void onEvent(Event event, Object source, Object data) {
    LOGGER.debug("onEvent: for '{}' on '{}'", event.getClass(), source);
    try {
      queueFromEvent(event, source);
    } catch (IllegalStateException ise) {
      LOGGER.error("failed to queue '{}': " + ise.getMessage(), source);
    }
  }

  private void queueFromEvent(Event event, Object source) {
    if (source == null) {
      LOGGER.error("onEvent: received null source");
    } else if ((event instanceof DocumentUpdatedEvent) || (event instanceof DocumentCreatedEvent)) {
      queueDocument((XWikiDocument) source, false);
    } else if (event instanceof DocumentDeletedEvent) {
      queueDocument((XWikiDocument) source, true);
    } else if ((event instanceof AttachmentUpdatedEvent)
        || (event instanceof AttachmentAddedEvent)) {
      queueAttachment(((XWikiDocument) source).getAttachment(
          ((AbstractAttachmentEvent) event).getName()), false);
    } else if (event instanceof AttachmentDeletedEvent) {
      queueAttachment((XWikiDocument) source, ((AbstractAttachmentEvent) event).getName(), true);
    } else if (event instanceof WikiDeletedEvent) {
      queueWiki(getModelUtils().resolveRef((String) source, WikiReference.class), true);
    }
  }

  /**
   * @return the number of documents in the queue.
   */
  public long getQueueSize() {
    return this.queue.getSize();
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

  private void notify(AbstractEntityEvent event) {
    Utils.getComponent(ObservationManager.class).notify(event, event.getReference(),
        getContext().getXWikiContext());
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
