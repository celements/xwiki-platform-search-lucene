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

import static java.util.concurrent.TimeUnit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.observation.ObservationManager;

import com.celements.common.observation.event.AbstractEntityEvent;
import com.celements.model.context.ModelContext;
import com.celements.model.util.ModelUtils;
import com.celements.search.lucene.index.DeleteData;
import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.extension.ILuceneIndexExtensionServiceRole;
import com.celements.search.lucene.index.queue.LuceneIndexingQueue;
import com.google.common.base.Stopwatch;
import com.xpn.xwiki.XWiki;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.XWikiException;
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

  static final String PROP_COMMIT_INTERVAL = "xwiki.plugins.lucene.commitinterval";

  /**
   * Collecting all the fields for using up in search
   */
  private static final Set<String> COLLECTED_FIELDS = ConcurrentHashMap.newKeySet();

  final LucenePlugin plugin;

  final IndexWriter writer;

  private final long commitInterval;

  private final LuceneIndexingQueue indexingQueue;

  private final AtomicBoolean exit = new AtomicBoolean(false);

  private final AtomicBoolean optimize = new AtomicBoolean(false);

  IndexUpdater(IndexWriter writer, LucenePlugin plugin) {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, getContext().getXWikiContext().clone());
    this.plugin = plugin;
    this.commitInterval = getWiki().ParamAsLong(PROP_COMMIT_INTERVAL, 5000);
    this.writer = writer;
    this.indexingQueue = Utils.getComponent(LuceneIndexingQueue.class);
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
    try (IndexCommitter indexCommitter = new IndexCommitter()) {
      while (!isExit()) {
        try (IndexCommitter commiter = indexCommitter) {
          indexData(indexingQueue.take());
        }
      }
    } catch (Exception exc) {
      LOGGER.error("Unexpected error occured", exc);
      doExit();
    }
    LOGGER.info("IndexUpdater finished");
  }

  private void indexData(IndexData data) {
    LOGGER.debug("indexData - '{}'", data);
    try {
      getContext().setWikiRef(data.getWikiRef());
      if (data.isDeleted()) {
        removeFromIndex((DeleteData) data);
      } else {
        addToIndex(data);
      }
    } catch (Exception exc) {
      LOGGER.error("error indexing document '{}'", data, exc);
    } finally {
      getContext().setWikiRef(getModelUtils().getMainWikiRef());
    }
  }

  private void addToIndex(IndexData data) throws IOException, XWikiException {
    LOGGER.debug("addToIndex: '{}'", data);
    EntityReference ref = data.getEntityReference();
    notify(data, new LuceneDocumentIndexingEvent(ref));
    Document luceneDoc = new Document();
    data.addDataToLuceneDocument(luceneDoc);
    getLuceneExtensionService().extend(data, luceneDoc);
    collectFields(luceneDoc);
    writer.updateDocument(data.getTerm(), luceneDoc);
    notify(data, new LuceneDocumentIndexedEvent(ref));
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

  private void removeFromIndex(DeleteData data) throws IOException {
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
    // optimize index if requested
    if (optimize.compareAndSet(true, false)) {
      LOGGER.warn("started optimizing lucene index");
      writer.optimize();
      LOGGER.warn("finished optimizing lucene index");
    }
    plugin.openSearchers();
  }

  private class IndexCommitter implements AutoCloseable {

    private final Stopwatch commitWatch = Stopwatch.createStarted();

    @Override
    public void close() throws IOException {
      if ((commitWatch.elapsed(MILLISECONDS) >= commitInterval) || indexingQueue.isEmpty()) {
        commitIndex();
      }
    }
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
    return Collections.unmodifiableSet(COLLECTED_FIELDS);
  }

  private void notify(IndexData data, AbstractEntityEvent event) {
    if (data.notifyObservationEvents()) {
      Utils.getComponent(ObservationManager.class).notify(event, event.getReference(),
          getContext().getXWikiContext());
    } else {
      LOGGER.debug("skip notify '{}' for '{}'", event, data);
    }
  }

  private static ILuceneIndexExtensionServiceRole getLuceneExtensionService() {
    return Utils.getComponent(ILuceneIndexExtensionServiceRole.class);
  }

  private static ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

  private static ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

  @Deprecated
  private static XWiki getWiki() {
    return getContext().getXWikiContext().getWiki();
  }

}
