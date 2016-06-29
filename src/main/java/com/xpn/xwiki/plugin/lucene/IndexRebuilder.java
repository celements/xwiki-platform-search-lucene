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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.context.Execution;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.query.Query;
import org.xwiki.query.QueryException;
import org.xwiki.query.QueryManager;

import com.celements.web.service.IWebUtilsService;
import com.google.common.base.Strings;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.doc.XWikiDocument;
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
   * Hibernate filtering query to reindex with.
   */
  private volatile String hqlFilter = null;

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

  private XWikiContext getContext() {
    return (XWikiContext) Utils.getComponent(Execution.class).getContext().getProperty(
        XWikiContext.EXECUTIONCONTEXT_KEY);
  }

  public boolean startIndexRebuild() {
    return startIndexRebuild(null, "", false);
  }

  public boolean startIndexRebuildWithWipe(List<WikiReference> wikis, String hqlFilter,
      boolean onlyNew) {
    this.wipeIndex = true;
    return startIndexRebuild(wikis, hqlFilter, onlyNew);
  }

  public boolean startIndexRebuild(List<WikiReference> wikis, String hqlFilter, boolean onlyNew) {
    if (rebuildInProgress.compareAndSet(false, true)) {
      this.wikis = getWikis(wikis);
      this.hqlFilter = hqlFilter;
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
      if (getContext().getWiki().isVirtualMode()) {
        try {
          for (String wiki : getContext().getWiki().getVirtualWikisDatabaseNames(getContext())) {
            ret.add(new WikiReference(wiki));
          }
          ret.add(new WikiReference(getContext().getMainXWiki()));
        } catch (XWikiException xwe) {
          LOGGER.error("failed to load virtual wiki names", xwe);
        }
        LOGGER.debug("found {} virtual wikis: '{}'", ret.size(), ret);
      } else {
        // No virtual wiki configuration, just index the wiki the context belongs to
        ret.add(getWebUtils().getWikiRef());
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
      context = getContext();
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
    for (WikiReference wikiRef : wikis) {
      String database = getContext().getDatabase();
      IndexSearcher searcher = null;
      try {
        getContext().setDatabase(wikiRef.getName());
        searcher = new IndexSearcher(indexUpdater.getDirectory(), true);
        retval += rebuildWiki(wikiRef, searcher);
      } catch (IOException | XWikiException | QueryException | InterruptedException exc) {
        LOGGER.error("Failed rebulding wiki [{}]", wikiRef, exc);
      } finally {
        getContext().setDatabase(database);
        IOUtils.closeQuietly(searcher);
      }
    }
    return retval;
  }

  private int rebuildWiki(WikiReference wikiRef, IndexSearcher searcher) throws IOException,
      XWikiException, QueryException, InterruptedException {
    LOGGER.info("rebuilding wiki '{}'", wikiRef);
    Set<String> remainingDocs = Collections.emptySet();
    if (wipeIndex) {
      wipeWikiIndex(wikiRef);
    } else if (StringUtils.isBlank(hqlFilter)) { // clean only possible if no hql filter set
      remainingDocs = getAllIndexedDocs(wikiRef, searcher);
    }
    int ret = 0, count = 0;
    List<Object[]> documentsToIndex = getAllDocs(wikiRef);
    for (Object[] docData : documentsToIndex) {
      DocumentReference docRef = new DocumentReference((String) docData[0], new SpaceReference(
          (String) docData[1], wikiRef));
      String version = (String) docData[2];
      String language = (String) docData[3];
      language = Strings.isNullOrEmpty(language) ? "default" : language;
      String docId = getWebUtils().serializeRef(docRef) + "." + language;
      if (!onlyNew || !isIndexed(docRef, version, language, searcher)) {
        ret += queueDocument(docRef, language);
        LOGGER.trace("indexed {}", docId);
      } else {
        LOGGER.trace("skipped '{}', already indexed", docId);
      }
      if (!remainingDocs.remove(docId)) {
        LOGGER.debug("couldn't reduce remaining docs for docId '{}'", docId);
      }
      if ((++count % 500) == 0) {
        LOGGER.info("indexed docs {}/{}, {} docs remaining", count, documentsToIndex.size(),
            remainingDocs.size());
      }
    }
    cleanIndex(remainingDocs);
    return ret;
  }

  private List<Object[]> getAllDocs(WikiReference wikiRef) throws QueryException {
    String hql = "select distinct doc.name, doc.space, doc.version, doc.language "
        + "from XWikiDocument as doc ";
    hqlFilter = hqlFilter.trim();
    if (StringUtils.isNotBlank(hqlFilter)) {
      if ((hqlFilter.charAt(0) != ',') && !hqlFilter.toLowerCase().contains("where")) {
        hql += "where ";
      }
      hql += hqlFilter;
    }
    Query query = getQueryManager().createQuery(hql, Query.HQL);
    query.setWiki(wikiRef.getName());
    List<Object[]> ret = query.execute();
    LOGGER.info("getAllDocs: {} to index with filter '{}'", ret.size(), hqlFilter);
    return ret;
  }

  public Set<String> getAllIndexedDocs(WikiReference wikiRef, IndexSearcher searcher)
      throws IOException {
    Set<String> ret = new HashSet<>();
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI, wikiRef.getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    TopDocs topDocs = searcher.search(query, Math.max(1, collector.getTotalHits()));
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      ret.add(searcher.doc(scoreDoc.doc).get(IndexFields.DOCUMENT_ID));
    }
    LOGGER.info("getAllIndexedDocs: found {} docs in index", ret.size());
    return ret;
  }

  private void wipeWikiIndex(WikiReference wikiRef) throws InterruptedException {
    waitForLowQueueSize();
    LOGGER.info("wipeWikiIndex: for '{}'", wikiRef);
    indexUpdater.queueWiki(wikiRef, true);
  }

  private void cleanIndex(Set<String> danglingDocs) throws InterruptedException {
    LOGGER.info("cleanIndex: {} for {} dangling docs", !wipeIndex, danglingDocs.size());
    for (String docId : danglingDocs) {
      waitForLowQueueSize();
      indexUpdater.queueDeletion(docId);
      LOGGER.trace("cleanIndex: deleted doc: {}", docId);
    }
  }

  protected int queueDocument(DocumentReference documentReference, String language)
      throws XWikiException, InterruptedException {
    int retval = 0;
    XWikiDocument document = getContext().getWiki().getDocument(documentReference, getContext());
    XWikiDocument tdocument = document.getTranslatedDocument(language, getContext());
    waitForLowQueueSize();
    indexUpdater.queueDocument(tdocument, false);
    ++retval;
    if (document == tdocument) {
      retval += indexUpdater.queueAttachments(document);
    }
    return retval;
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
      getContext().getWiki().getStore().cleanUp(getContext());
      LOGGER.debug("sleeping for {}ms since queue size {} too big", retryInterval, size);
      Thread.sleep(retryInterval);
    }
  }

  public boolean isIndexed(DocumentReference docRef, IndexSearcher searcher) throws IOException {
    return isIndexed(docRef, null, null, searcher);
  }

  public boolean isIndexed(DocumentReference docRef, String version, String language,
      IndexSearcher searcher) throws IOException {
    boolean exists = false;
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_NAME, docRef.getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_SPACE,
        docRef.getLastSpaceReference().getName().toLowerCase())), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI,
        docRef.getWikiReference().getName().toLowerCase())), BooleanClause.Occur.MUST);
    if (version != null) {
      query.add(new TermQuery(new Term(IndexFields.DOCUMENT_VERSION, version)),
          BooleanClause.Occur.MUST);
    }
    if (language != null) {
      query.add(new TermQuery(new Term(IndexFields.DOCUMENT_LANGUAGE, language)),
          BooleanClause.Occur.MUST);
    }
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    exists = collector.getTotalHits() > 0;
    return exists;
  }

  private QueryManager getQueryManager() {
    return Utils.getComponent(QueryManager.class);
  }

  private IWebUtilsService getWebUtils() {
    return Utils.getComponent(IWebUtilsService.class);
  }

}
