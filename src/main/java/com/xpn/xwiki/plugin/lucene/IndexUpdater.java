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

import org.apache.commons.io.IOUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
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

  /**
   * Logging helper.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexUpdater.class);

  private static final String NAME = "lucene";

  /**
   * The maximum number of milliseconds we have to wait before this thread is safely
   * closed.
   */
  private static final int EXIT_INTERVAL = 3000;

  private static final List<Event> EVENTS = Arrays.<Event>asList(new DocumentUpdatedEvent(),
      new DocumentCreatedEvent(), new DocumentDeletedEvent(), new AttachmentAddedEvent(),
      new AttachmentDeletedEvent(), new AttachmentUpdatedEvent());

  /**
   * Collecting all the fields for using up in search
   */
  private static final Set<String> COLLECTED_FIELDS = Collections.newSetFromMap(
      new ConcurrentHashMap<String, Boolean>());

  private final LucenePlugin plugin;

  private final IndexWriter writer;

  /**
   * Milliseconds of sleep between checks for changed documents.
   */
  private final int indexingInterval;

  private final Directory directory;

  private final XWikiDocumentQueue queue = new XWikiDocumentQueue();

  /**
   * Milliseconds left till the next check for changed documents.
   */
  private int indexingTimer = 0;

  /**
   * Soft threshold after which no more documents will be added to the indexing queue.
   * When the queue size gets larger than this value, the index rebuilding thread will
   * sleep chunks of {@code IndexRebuilder#retryInterval} milliseconds until the queue
   * size will get back bellow this threshold. This does not affect normal indexing
   * through wiki updates.
   */
  private final int maxQueueSize;

  private final int commitInterval;

  private final AtomicBoolean exit = new AtomicBoolean(false);

  private final AtomicBoolean optimize = new AtomicBoolean(false);

  private long lastCommitTime;

  IndexUpdater(Directory directory, int indexingInterval, int maxQueueSize, int commitInterval,
      LucenePlugin plugin, XWikiContext context) throws IOException {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, context.clone());
    this.plugin = plugin;
    this.directory = directory;
    this.indexingInterval = indexingInterval;
    this.maxQueueSize = maxQueueSize;
    this.commitInterval = commitInterval;
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

  public IndexWriter getWriter() {
    return writer;
  }

  IndexWriter openWriter(OpenMode openMode) throws IOException {
    IndexWriter ret = null;
    while (ret == null) {
      try {
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34, plugin.getAnalyzer());
        // cfg.setRAMBufferSizeMB(ramBufferSizeMB); TODO
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
    while (!exit.get()) {
      try {
        // Check if the indexing interval elapsed.
        if (this.indexingTimer == 0) {
          // Reset the indexing timer.
          this.indexingTimer = this.indexingInterval;
          pollIndexQueue(); // Poll the queue for documents to be indexed
          optimizeIndex(); // optimize index if requested
        }
        // Remove the exit interval from the indexing timer.
        int sleepInterval = Math.min(EXIT_INTERVAL, this.indexingTimer);
        this.indexingTimer -= sleepInterval;
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
      getWriter().optimize();
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
    getWriter().updateDocument(data.getTerm(), luceneDoc);
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
    notify(new LuceneDocumentDeletingEvent(ref));
    getWriter().deleteDocuments(data.getTerm());
    notify(new LuceneDocumentDeletedEvent(ref));
  }

  public void commitIndex() throws IOException {
    getWriter().commit();
    plugin.openSearchers(getContext());
  }

  /**
   * ATTENTION: this call wipes the index from the disk, use with caution
   */
  // TODO this will always block right now
  public void wipeIndex() {
    LOGGER.info("trying to wipe index for rebuilding");
    try {
      IOUtils.closeQuietly(openWriter(OpenMode.CREATE));
    } catch (IOException e) {
      LOGGER.error("Failed to wipe index", e);
    }
  }

  public void queueDocument(XWikiDocument document, XWikiContext context, boolean deleted) {
    LOGGER.debug("IndexUpdater: adding '{}' to queue ",
        document.getDocumentReference().getLastSpaceReference().getName() + "."
            + document.getDocumentReference().getName());
    this.queue.add(new DocumentData(document, context, deleted));
    LOGGER.debug("IndexUpdater: queue has now size " + getQueueSize() + ", is empty: "
        + queue.isEmpty());
  }

  public void queueAttachment(XWikiAttachment attachment, XWikiContext context, boolean deleted) {
    if ((attachment != null) && (context != null)) {
      this.queue.add(new AttachmentData(attachment, context, deleted));
    } else {
      LOGGER.error("Invalid parameters given to {} attachment '{}' of document '{}'", new Object[] {
          deleted ? "deleted" : "added", attachment == null ? null : attachment.getFilename(),
          (attachment == null) || (attachment.getDoc() == null) ? null
              : attachment.getDoc().getDocumentReference() });
    }
  }

  public void addAttachment(XWikiDocument document, String attachmentName, XWikiContext context,
      boolean deleted) {
    if ((document != null) && (attachmentName != null) && (context != null)) {
      this.queue.add(new AttachmentData(document, attachmentName, context, deleted));
    } else {
      LOGGER.error("Invalid parameters given to {} attachment '{}' of document '{}'", new Object[] {
          (deleted ? "deleted" : "added"), attachmentName, document });
    }
  }

  public void addWiki(String wikiId, boolean deleted) {
    if (wikiId != null) {
      this.queue.add(new WikiData(new WikiReference(wikiId), deleted));
    } else {
      LOGGER.error("Invalid parameters given to {} wiki '{}'", (deleted ? "deleted" : "added"),
          wikiId);
    }
  }

  public int queueAttachments(XWikiDocument document, XWikiContext context) {
    int retval = 0;

    final List<XWikiAttachment> attachmentList = document.getAttachmentList();
    retval += attachmentList.size();
    for (XWikiAttachment attachment : attachmentList) {
      try {
        queueAttachment(attachment, context, false);
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
    XWikiContext context = (XWikiContext) data;
    LOGGER.debug("IndexUpdater: onEvent for [" + event.getClass() + "] on [" + source.toString()
        + "].");
    try {
      if ((event instanceof DocumentUpdatedEvent) || (event instanceof DocumentCreatedEvent)) {
        queueDocument((XWikiDocument) source, context, false);
      } else if (event instanceof DocumentDeletedEvent) {
        queueDocument((XWikiDocument) source, context, true);
      } else if ((event instanceof AttachmentUpdatedEvent)
          || (event instanceof AttachmentAddedEvent)) {
        queueAttachment(((XWikiDocument) source).getAttachment(
            ((AbstractAttachmentEvent) event).getName()), context, false);
      } else if (event instanceof AttachmentDeletedEvent) {
        addAttachment((XWikiDocument) source, ((AbstractAttachmentEvent) event).getName(), context,
            true);
      } else if (event instanceof WikiDeletedEvent) {
        addWiki((String) source, true);
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
  // FIXME CELDEV-275 this method blocks the IndexUpdater completely because it keeps a writer
  // opened while counting
  // TODO why is writer used for this?
  public long getLuceneDocCount() {
    int n = -1;
    try {
      n = getWriter().numDocs();
    } catch (IOException e) {
      LOGGER.error("Failed to get the number of documents in Lucene index writer", e);
    }
    return n;
  }

  public int getMaxQueueSize() {
    return this.maxQueueSize;
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
