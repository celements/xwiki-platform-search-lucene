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

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.suigeneris.jrcs.rcs.Version;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.query.QueryException;

import com.celements.model.access.IModelAccessFacade;
import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.model.context.ModelContext;
import com.celements.model.metadata.DocumentMetaData;
import com.celements.model.util.References;
import com.celements.search.lucene.index.AttachmentData;
import com.celements.search.lucene.index.DeleteData;
import com.celements.search.lucene.index.DocumentData;
import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.LuceneDocId;
import com.celements.search.lucene.index.WikiData;
import com.celements.store.DocumentCacheStore;
import com.celements.store.MetaDataStoreExtension;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.store.XWikiCacheStoreInterface;
import com.xpn.xwiki.util.AbstractXWikiRunnable;
import com.xpn.xwiki.web.Utils;

/**
 * <p>
 * Handles rebuilding of the whole Lucene Search Index. This involves the following steps:
 * <ul>
 * <li>empty the existing index</li>
 * <li>retrieve the names of all virtual wikis</li>
 * <li>foreach document in each virtual wiki:
 * <ul>
 * <li>index the document</li>
 * <li>get and index all translations of the document</li>
 * <li>get and index all attachments of the document</li>
 * <li>get and index all objects of the document</li>
 * </ul>
 * </li>
 * </ul>
 * The rebuild can be triggered using the {@link LucenePluginApi#rebuildIndex()} method of
 * the {@link LucenePluginApi}. Once a rebuild request is made, a new thread is created,
 * so the requesting script can continue processing, while the rebuilding is done in the
 * background. The actual indexing is done by the IndexUpdater thread, this thread just
 * gathers the data and passes it to the IndexUpdater.
 * </p>
 * <p>
 * As a summary, this plugin:
 * <ul>
 * <li>cleans the Lucene search indexes and re-submits all the contents of all the wikis
 * for indexing</li>
 * <li>without clogging the indexing thread (since 1.2)</li>
 * <li>all in a background thread (since 1.2)</li>
 * <li>making sure that only one rebuild is in progress (since 1.2)</li>
 * </ul>
 * </p>
 *
 * @version $Id: 5bb91a92a5990405edd8203dff5b4e24103af5c3 $
 */
public class IndexRebuilder extends AbstractXWikiRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexRebuilder.class);

  static final String PROP_UPDATER_RETRY_INTERVAL = "xwiki.plugins.lucene.updaterRetryInterval";

  static final String PROP_MAX_QUEUE_SIZE = "xwiki.plugins.lucene.maxQueueSize";

  /**
   * Amount of time (milliseconds) to sleep while waiting for the indexing queue to empty.
   */
  private final int retryInterval;

  /**
   * Soft threshold after which no more documents will be added to the indexing queue.
   * When the queue size gets larger than this value, the index rebuilding thread will
   * sleep chunks of {@code IndexRebuilder#retryInterval} milliseconds until the queue
   * size will get back bellow this threshold. This does not affect normal indexing
   * through wiki updates.
   */
  private final long maxQueueSize;

  /**
   * The actual object/thread that indexes data.
   */
  private final IndexUpdater indexUpdater;

  /**
   * Variable used for indicating that a rebuild is already in progress.
   */
  private final AtomicBoolean rebuildInProgress = new AtomicBoolean(false);

  /**
   * Wikis where to search.
   */
  private volatile List<WikiReference> wikis = null;

  /**
   * Reference to filter reindex data with.
   */
  private volatile Optional<EntityReference> filterRef = Optional.absent();

  /**
   * Indicate if document already in the Lucene index are updated.
   */
  private volatile boolean onlyNew = false;

  /**
   * Indicate if the Lucene index should be wiped, if false index will be cleaned from inexistent
   * documents.
   */
  private volatile boolean wipeIndex = false;

  public IndexRebuilder(IndexUpdater indexUpdater, XWikiContext context) {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, context.clone());
    this.indexUpdater = indexUpdater;
    this.retryInterval = 1000 * (int) context.getWiki().ParamAsLong(PROP_UPDATER_RETRY_INTERVAL,
        30);
    this.maxQueueSize = context.getWiki().ParamAsLong(PROP_MAX_QUEUE_SIZE, 1000);
  }

  public boolean startIndexRebuildWithWipe(List<WikiReference> wikis, boolean onlyNew) {
    this.wipeIndex = true;
    return startIndexRebuild(wikis, Optional.<EntityReference>absent(), onlyNew);
  }

  public boolean startIndexRebuild(List<WikiReference> wikis, Optional<EntityReference> filterRef,
      boolean onlyNew) {
    if (rebuildInProgress.compareAndSet(false, true)) {
      this.wikis = getWikis(wikis);
      this.filterRef = filterRef;
      this.onlyNew = onlyNew;
      Thread indexRebuilderThread = new Thread(this, "Lucene Index Rebuilder");
      indexRebuilderThread.setDaemon(true); // The JVM should be allowed to shutdown while this
                                            // thread is running
      indexRebuilderThread.setPriority(3);// Client requests are more important than indexing
      indexRebuilderThread.start();
      return true;
    } else {
      LOGGER.warn("Cannot launch rebuild because another rebuild is in progress");
      return false;
    }
  }

  private List<WikiReference> getWikis(List<WikiReference> wikis) {
    if (wikis != null) {
      return wikis;
    } else {
      Set<WikiReference> ret = new HashSet<>();
      if (getContext().getXWikiContext().getWiki().isVirtualMode()) {
        try {
          for (String wiki : getContext().getXWikiContext().getWiki().getVirtualWikisDatabaseNames(
              getContext().getXWikiContext())) {
            ret.add(new WikiReference(wiki));
          }
          ret.add(getContext().getMainWikiRef());
        } catch (XWikiException xwe) {
          LOGGER.error("failed to load virtual wiki names", xwe);
        }
        LOGGER.debug("found {} virtual wikis: '{}'", ret.size(), ret);
      } else {
        // No virtual wiki configuration, just index the wiki the context belongs to
        ret.add(getContext().getWikiRef());
      }
      return new ArrayList<>(ret);
    }
  }

  @Override
  protected void runInternal() {
    LOGGER.info("Starting lucene index rebuild");
    XWikiContext context = null;
    try {
      // The context must be cloned, as otherwise setDatabase() might affect the response
      // to
      // the current request.
      // TODO This is not a good way to do this; ideally there would be a method that
      // creates
      // a new context and copies only a few needed objects, as some objects are not
      // supposed
      // to be used in 2 different contexts.
      // TODO This seems to work on a simple run:
      // context = new XWikiContext();
      // context.setWiki(this.context.getWiki());
      // context.setEngineContext(this.context.getEngineContext());
      // context.setMode(this.context.getMode());
      // context.setAction(this.context.getAction());
      // context.put("msg", this.context.get("msg"));
      // context.setMainXWiki(this.context.getMainXWiki());
      // context.setURLFactory(this.context.getURLFactory());
      // context.setLanguage(this.context.getLanguage());
      // context.setDatabase(this.context.getDatabase());
      // context.put("org.xwiki.component.manager.ComponentManager", this.context
      // .get("org.xwiki.component.manager.ComponentManager"));
      context = getContext().getXWikiContext();
      // For example, we definitely don't want to use the same hibernate session...
      context.remove("hibsession");
      context.remove("hibtransaction");
      // This is also causing serious problems, as the same xcontext gets shared between
      // threads and causes the hibernate session to be shared in the end. The vcontext is
      // automatically recreated by the velocity renderer, if it isn't found in the
      // xcontext.
      context.remove("vcontext");

      // The original request and response should not be used outside the actual request
      // processing thread, as they will be cleaned later by the container.
      context.setRequest(null);
      context.setResponse(null);

      rebuildIndex();
    } catch (Exception e) {
      LOGGER.error("Error in lucene rebuild thread: {}", e.getMessage(), e);
    } finally {
      if (context != null) {
        context.getWiki().getStore().cleanUp(context);
      }
      rebuildInProgress.set(false);
    }

    LOGGER.info("Lucene index rebuild done");
  }

  /**
   * First empties the index, then fetches all Documents, their translations and their
   * attachments for re-addition to the index.
   *
   * @return the number of indexed elements
   */
  private int rebuildIndex() {
    int retval = 0;
    WikiReference beforeWikiRef = getContext().getWikiRef();
    for (WikiReference wikiRef : wikis) {
      IndexSearcher searcher = null;
      try {
        getContext().setWikiRef(wikiRef);
        searcher = new IndexSearcher(indexUpdater.getDirectory(), true);
        retval += rebuildWiki(wikiRef, searcher);
      } catch (IOException | QueryException | InterruptedException exc) {
        LOGGER.error("Failed rebulding wiki [{}]", wikiRef, exc);
      } finally {
        getContext().setWikiRef(beforeWikiRef);
        IOUtils.closeQuietly(searcher);
      }
    }
    return retval;
  }

  private int rebuildWiki(@NotNull WikiReference wikiRef, @NotNull IndexSearcher searcher)
      throws IOException, QueryException, InterruptedException {
    EntityReference filterRef = this.filterRef.or(checkNotNull(wikiRef));
    Preconditions.checkArgument(References.extractRef(filterRef, WikiReference.class).get().equals(
        wikiRef), "unable to index wiki '" + wikiRef + "' for set filter '" + filterRef + "'");
    LOGGER.info("rebuilding wiki '{}'", wikiRef);
    int ret = 0, count = 0;
    Set<LuceneDocId> docsInIndex = getAllIndexedDocs(filterRef, searcher);
    Set<DocumentMetaData> docsToIndex = getAllDocMetaData(filterRef);
    for (DocumentMetaData metaData : docsToIndex) {
      LuceneDocId docId = getDocId(metaData);
      if (!onlyNew || !isIndexed(metaData, searcher)) {
        ret += queueDocument(metaData);
        LOGGER.trace("indexed {}", docId);
      } else {
        LOGGER.trace("skipped '{}', already indexed", docId);
      }
      if (!docsInIndex.remove(docId)) {
        LOGGER.debug("couldn't reduce remaining docs for docId '{}'", docId);
      }
      if ((++count % 500) == 0) {
        LOGGER.info("indexed docs {}/{}, {} docs remaining", count, docsToIndex.size(),
            docsInIndex.size());
      }
    }
    cleanIndex(docsInIndex);
    return ret;
  }

  private Set<DocumentMetaData> getAllDocMetaData(@NotNull EntityReference ref)
      throws QueryException {
    MetaDataStoreExtension store;
    if (getContext().getXWikiContext().getWiki().getStore() instanceof MetaDataStoreExtension) {
      store = (MetaDataStoreExtension) getContext().getXWikiContext().getWiki().getStore();
    } else {
      store = (MetaDataStoreExtension) Utils.getComponent(XWikiCacheStoreInterface.class,
          DocumentCacheStore.COMPONENT_NAME);
    }
    return store.listDocumentMetaData(ref);
  }

  public Set<LuceneDocId> getAllIndexedDocs(@NotNull EntityReference ref,
      @NotNull IndexSearcher searcher) throws IOException, InterruptedException {
    Set<LuceneDocId> ret = new HashSet<>();
    if (wipeIndex) {
      wipeWikiIndex(ref);
    } else {
      Query query = getLuceneSearchRefQuery(ref);
      TotalHitCountCollector collector = new TotalHitCountCollector();
      searcher.search(query, collector);
      TopDocs topDocs = searcher.search(query, Math.max(1, collector.getTotalHits()));
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        try {
          SearchResult result = new SearchResult(searcher.doc(scoreDoc.doc), scoreDoc.score);
          ret.add(result.getId());
        } catch (IllegalArgumentException iae) {
          LOGGER.warn("encountered invalid doc in index: {}", scoreDoc, iae);
        }
      }
    }
    LOGGER.info("getAllIndexedDocs: found {} docs in index for ref '{}'", ret.size(), ref);
    return ret;
  }

  private void wipeWikiIndex(@NotNull EntityReference ref) throws InterruptedException {
    WikiReference wikiRef = References.extractRef(ref, WikiReference.class).get();
    waitForLowQueueSize();
    LOGGER.info("wipeWikiIndex: for '{}'", wikiRef);
    queue(new WikiData(wikiRef, true));
  }

  private void cleanIndex(Set<LuceneDocId> danglingDocs) throws InterruptedException {
    LOGGER.info("cleanIndex: {} for {} dangling docs", !wipeIndex, danglingDocs.size());
    danglingDocs.remove(null);
    for (LuceneDocId docId : danglingDocs) {
      waitForLowQueueSize();
      queue(new DeleteData(docId));
      LOGGER.trace("cleanIndex: deleted doc: {}", docId);
    }
  }

  protected int queueDocument(DocumentMetaData metaData) throws InterruptedException {
    int retval = 0;
    try {
      XWikiDocument doc = getModelAccess().getDocument(metaData.getDocRef(),
          metaData.getLanguage());
      waitForLowQueueSize();
      queue(new DocumentData(doc, false));
      ++retval;
      if (!getModelAccess().isTranslation(doc)) {
        for (XWikiAttachment att : doc.getAttachmentList()) {
          queue(new AttachmentData(att, false));
          ++retval;
        }
      }
    } catch (DocumentNotExistsException exc) {
      LOGGER.warn("failed to queue doc '{}'", metaData);
    }
    return retval;
  }

  private void queue(IndexData data) {
    data.disableObservationEventNotification();
    indexUpdater.queue(data);
  }

  // In order not to load the whole database in memory, we're limiting the number
  // of documents that are in the processing queue at a moment. We could use a
  // Bounded Queue in the index updater, but that would generate exceptions in the
  // rest of the platform, as the index rebuilder could fill the queue, and then a
  // user trying to save a document would cause an exception. Thus, it is better
  // to limit the index rebuilder thread only, and not the index updater.
  private void waitForLowQueueSize() throws InterruptedException {
    long size;
    while ((size = indexUpdater.getQueueSize()) > maxQueueSize) {
      // Don't leave any database connections open while sleeping
      // This shouldn't be needed, but we never know what bugs might be there
      getContext().getXWikiContext().getWiki().getStore().cleanUp(getContext().getXWikiContext());
      LOGGER.debug("sleeping for {}ms since queue size {} too big", retryInterval, size);
      Thread.sleep(retryInterval);
    }
  }

  public boolean isIndexed(DocumentReference docRef, IndexSearcher searcher) throws IOException {
    return isIndexed(docRef, null, null, searcher);
  }

  public boolean isIndexed(DocumentMetaData metaData, IndexSearcher searcher) throws IOException {
    return isIndexed(metaData.getDocRef(), metaData.getLanguage(), metaData.getVersion(), searcher);
  }

  public boolean isIndexed(DocumentReference docRef, String language, Version version,
      IndexSearcher searcher) throws IOException {
    boolean exists = false;
    BooleanQuery query = getLuceneSearchRefQuery(docRef);
    language = Strings.isNullOrEmpty(language) ? "default" : language;
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_LANGUAGE, language)),
        BooleanClause.Occur.MUST);
    if (version != null) {
      query.add(new TermQuery(new Term(IndexFields.DOCUMENT_VERSION, version.toString())),
          BooleanClause.Occur.MUST);
    }
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    exists = collector.getTotalHits() > 0;
    return exists;
  }

  private BooleanQuery getLuceneSearchRefQuery(@NotNull EntityReference ref) {
    BooleanQuery query = new BooleanQuery();
    Optional<DocumentReference> docRef = References.extractRef(ref, DocumentReference.class);
    if (docRef.isPresent()) {
      query.add(new TermQuery(new Term(IndexFields.DOCUMENT_NAME,
          docRef.get().getName().toLowerCase())), BooleanClause.Occur.MUST);
    }
    Optional<SpaceReference> spaceRef = References.extractRef(ref, SpaceReference.class);
    if (docRef.isPresent()) {
      query.add(new TermQuery(new Term(IndexFields.DOCUMENT_SPACE,
          spaceRef.get().getName().toLowerCase())), BooleanClause.Occur.MUST);
    }
    WikiReference wikiRef = References.extractRef(ref, WikiReference.class).get();
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI, wikiRef.getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    return query;
  }

  static LuceneDocId getDocId(DocumentMetaData metaData) {
    return new LuceneDocId(metaData.getDocRef(), metaData.getLanguage());
  }

  private static IModelAccessFacade getModelAccess() {
    return Utils.getComponent(IModelAccessFacade.class);
  }

  private static ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

}
