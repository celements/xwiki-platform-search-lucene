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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.bridge.event.DocumentCreatedEvent;
import org.xwiki.bridge.event.DocumentDeletedEvent;
import org.xwiki.bridge.event.DocumentUpdatedEvent;
import org.xwiki.bridge.event.WikiDeletedEvent;
import org.xwiki.context.Execution;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.observation.EventListener;
import org.xwiki.observation.ObservationManager;
import org.xwiki.observation.event.Event;

import com.celements.common.observation.event.AbstractEntityEvent;
import com.celements.web.service.IWebUtilsService;
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

  static final String PROP_WRITER_BUFFER_SIZE = "xwiki.plugins.lucene.writerBufferSize";

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

  private final LucenePlugin plugin;

  private final Directory directory;

  private final IndexWriter writer;

  /**
   * Milliseconds of sleep between checks for changed documents.
   */
  private final long indexingInterval;

  private final long commitInterval;

  private final XWikiDocumentQueue queue = new XWikiDocumentQueue();

  private final AtomicBoolean exit = new AtomicBoolean(false);

  private final AtomicBoolean optimize = new AtomicBoolean(false);

  IndexUpdater(Directory directory, LucenePlugin plugin, XWikiContext context) throws IOException {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, context.clone());
    this.plugin = plugin;
    this.directory = directory;
    this.indexingInterval = 1000 * context.getWiki().ParamAsLong(PROP_INDEXING_INTERVAL, 30);
    this.commitInterval = context.getWiki().ParamAsLong(PROP_COMMIT_INTERVAL, 1000);
    this.writer = openWriter(OpenMode.CREATE_OR_APPEND);
  }

  private XWikiContext getContext() {
    return (XWikiContext) Utils.getComponent(Execution.class).getContext().getProperty(
        XWikiContext.EXECUTIONCONTEXT_KEY);
  }

  public void doExit() {
    exit.set(true);
  }

  public void doOptimize() {
    optimize.set(true);
  }

  IndexWriter openWriter(OpenMode openMode) throws IOException {
    IndexWriter ret = null;
    while (ret == null) {
      try {
        IndexWriterConfig cfg = new IndexWriterConfig(LucenePlugin.VERSION, plugin.getAnalyzer());
        cfg.setRAMBufferSizeMB(getContext().getWiki().ParamAsLong(PROP_WRITER_BUFFER_SIZE,
            (long) IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB));
        if (openMode != null) {
          cfg.setOpenMode(openMode);
        }
        ret = new IndexWriter(getDirectory(), cfg);
      } catch (LockObtainFailedException exc) {
        try {
          int ms = new Random().nextInt(1000);
          LOGGER.debug("failed to acquire lock, retrying in {}ms ...", ms);
          Thread.sleep(ms);
        } catch (InterruptedException ex) {
          LOGGER.warn("Error while sleeping", ex);
        }
      }
    }
    return ret;
  }

  /**
   * Return a reference to the directory that this updater is currently working with.
   */
  public Directory getDirectory() {
    return this.directory;
  }

  /**
   * Main loop. Polls the queue for documents to be indexed.
   *
   * @see java.lang.Runnable#run()
   */
  @Override
  protected void runInternal() {
    getContext().setDatabase(getContext().getMainXWiki());
    runMainLoop();
  }

  /**
   * Main loop. Polls the queue for documents to be indexed.
   */
  private void runMainLoop() {
    long indexingTimer = 0;
    while (!exit.get()) {
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
      } catch (IOException ioExc) {
        LOGGER.error("failed to update index", ioExc);
        doExit();
      } catch (InterruptedException exc) {
        LOGGER.warn("Error while sleeping", exc);
      }
    }
  }

  private void optimizeIndex() throws IOException {
    if (optimize.compareAndSet(true, false)) {
      writer.optimize();
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
      LOGGER.debug("pollIndexQueue: documents in queue, start indexing");
      String curDB = getContext().getDatabase();
      try {
        updateIndex();
      } finally {
        getContext().setDatabase(curDB);
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
        indexData(data);
        hasUncommitedWrites = true;
        if ((System.currentTimeMillis() - lastCommitTime) >= commitInterval) {
          commitIndex();
          lastCommitTime = System.currentTimeMillis();
          hasUncommitedWrites = false;
        }
      } catch (IOException | XWikiException exc) {
        LOGGER.error("error indexing document '{}'", data, exc);
      }
    }
    if (hasUncommitedWrites) {
      commitIndex();
    }
    LOGGER.info("updateIndex finished");
  }

  private void indexData(AbstractIndexData data) throws IOException, XWikiException {
    getContext().setDatabase(getWebUtils().getWikiRef(data.getEntityReference()).getName());
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
    data.addDataToLuceneDocument(luceneDoc, getContext());
    getLuceneExtensionService().extend(data, luceneDoc);
    collectFields(luceneDoc);
    writer.updateDocument(data.getTerm(), luceneDoc);
    notify(new LuceneDocumentIndexedEvent(ref));
  }

  // collecting all the fields for using up in search
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
    writer.commit();
    plugin.openSearchers();
  }

  public void queueDeletion(String docId) {
    LOGGER.debug("IndexUpdater: adding '{}' to queue", docId);
    this.queue.add(new DeleteData(docId));
    LOGGER.debug("IndexUpdater: queue has now size " + getQueueSize() + ", is empty: "
        + queue.isEmpty());
  }

  public void queueDocument(XWikiDocument document, boolean deleted) {
    LOGGER.debug("IndexUpdater: adding '{}' to queue ",
        document.getDocumentReference().getLastSpaceReference().getName() + "."
            + document.getDocumentReference().getName());
    this.queue.add(new DocumentData(document, getContext(), deleted));
    LOGGER.debug("IndexUpdater: queue has now size " + getQueueSize() + ", is empty: "
        + queue.isEmpty());
  }

  public void queueAttachment(XWikiAttachment attachment, boolean deleted) {
    if (attachment != null) {
      this.queue.add(new AttachmentData(attachment, getContext(), deleted));
    } else {
      LOGGER.error("Invalid parameters given to {} attachment '{}' of document '{}'", new Object[] {
          deleted ? "deleted" : "added", attachment == null ? null : attachment.getFilename(),
          (attachment == null) || (attachment.getDoc() == null) ? null
              : attachment.getDoc().getDocumentReference() });
    }
  }

  public void queueAttachment(XWikiDocument document, String attachmentName, boolean deleted) {
    if ((document != null) && (attachmentName != null)) {
      this.queue.add(new AttachmentData(document, attachmentName, getContext(), deleted));
    } else {
      LOGGER.error("Invalid parameters given to {} attachment '{}' of document '{}'", new Object[] {
          (deleted ? "deleted" : "added"), attachmentName, document });
    }
  }

  public void queueWiki(WikiReference wikiRef, boolean deleted) {
    if (wikiRef != null) {
      this.queue.add(new WikiData(wikiRef, deleted));
    } else {
      LOGGER.error("Invalid parameters given to {} wiki '{}'", (deleted ? "deleted" : "added"),
          wikiRef);
    }
  }

  public int queueAttachments(XWikiDocument document) {
    int retval = 0;

    final List<XWikiAttachment> attachmentList = document.getAttachmentList();
    retval += attachmentList.size();
    for (XWikiAttachment attachment : attachmentList) {
      try {
        queueAttachment(attachment, false);
      } catch (Exception e) {
        LOGGER.error("Failed to retrieve attachment '{}' of document '{}'", new Object[] {
            attachment.getFilename(), document, e });
      }
    }

    return retval;
  }

  // @Override
  @Override
  public String getName() {
    return NAME;
  }

  // @Override
  @Override
  public List<Event> getEvents() {
    return EVENTS;
  }

  // @Override
  @Override
  public void onEvent(Event event, Object source, Object data) {
    LOGGER.debug("IndexUpdater: onEvent for [" + event.getClass() + "] on [" + source.toString()
        + "].");
    try {
      if ((event instanceof DocumentUpdatedEvent) || (event instanceof DocumentCreatedEvent)) {
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
        queueWiki(getWebUtils().resolveReference((String) source, WikiReference.class), true);
      }
    } catch (Exception e) {
      LOGGER.error("error in notify", e);
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
    Utils.getComponent(ObservationManager.class).notify(event, event.getReference(), getContext());
  }

  public ILuceneIndexExtensionServiceRole getLuceneExtensionService() {
    return Utils.getComponent(ILuceneIndexExtensionServiceRole.class);
  }

  public IWebUtilsService getWebUtils() {
    return Utils.getComponent(IWebUtilsService.class);
  }

}
