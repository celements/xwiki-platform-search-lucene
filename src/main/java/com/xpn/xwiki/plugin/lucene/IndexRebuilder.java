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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.context.Execution;
import org.xwiki.model.reference.DocumentReference;

import com.celements.web.service.IWebUtilsService;
import com.xpn.xwiki.XWiki;
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

  /**
   * Logging helper.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexRebuilder.class);

  /**
   * Amount of time (milliseconds) to sleep while waiting for the indexing queue to empty.
   */
  private static final int RETRYINTERVAL = 30000;

  /**
   * The actual object/thread that indexes data.
   */
  private final IndexUpdater indexUpdater;

  /**
   * Variable used for indicating that a rebuild is already in progress.
   */
  private volatile boolean rebuildInProgress = false;

  /**
   * Wikis where to search.
   */
  private Collection<String> wikis = null;

  /**
   * Hibernate filtering query to reindex with.
   */
  private String hqlFilter = null;

  /**
   * Indicate if document already in the Lucene index are updated.
   */
  private boolean onlyNew = false;

  /**
   * Indicate if the Lucene index should be cleaned from inexistent documents.
   */
  private boolean cleanIndex = false;

  public IndexRebuilder(IndexUpdater indexUpdater, XWikiContext context) {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, context.clone());

    this.indexUpdater = indexUpdater;
  }

  private XWikiContext getContext() {
    return (XWikiContext) Utils.getComponent(Execution.class).getContext()
        .getProperty(XWikiContext.EXECUTIONCONTEXT_KEY);
  }

  public int startRebuildIndex(XWikiContext context) {
    return startIndex(null, "", true, false, context);
  }

  public synchronized int startIndex(Collection<String> wikis, String hqlFilter,
      boolean clearIndex, boolean onlyNew, XWikiContext context) {
    if (this.rebuildInProgress) {
      LOGGER.warn("Cannot launch rebuild because another rebuild is in progress");
      return LucenePluginApi.REBUILD_IN_PROGRESS;
    } else {
      if (clearIndex) {
        clearIndex(wikis);
      }
      this.wikis = wikis != null ? new ArrayList<String>(wikis) : null;
      this.hqlFilter = hqlFilter;
      this.onlyNew = onlyNew;
      this.cleanIndex = !clearIndex;
      this.rebuildInProgress = true;

      Thread indexRebuilderThread = new Thread(this, "Lucene Index Rebuilder");
      // The JVM should be allowed to shutdown while this thread is running
      indexRebuilderThread.setDaemon(true);
      // Client requests are more important than indexing
      indexRebuilderThread.setPriority(3);
      // Finally, start the rebuild in the background
      indexRebuilderThread.start();

      // Too bad that now we can't tell how many items are there to be indexed...
      return 0;
    }
  }

  private void clearIndex(Collection<String> wikis) {
    if (wikis == null) {
      this.indexUpdater.cleanIndex();
    } else {
      IndexWriter writer = null;
      try {
        writer = this.indexUpdater.openWriter(false);
        for (String wiki : wikis) {
          writer.deleteDocuments(new Term(IndexFields.DOCUMENT_WIKI, wiki));
        }
      } catch (IOException ex) {
        LOGGER.warn("Failed to clear wiki index: {}", ex.getMessage());
      } finally {
        IOUtils.closeQuietly(writer);
      }
    }
  }

  @Override
  protected void runInternal() {
    LOGGER.debug("Starting lucene index rebuild");
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

      rebuildIndex(context);
    } catch (InterruptedException e) {
      LOGGER.warn("The index rebuilder thread has been interrupted");
    } catch (Exception e) {
      LOGGER.error("Error in lucene rebuild thread: {}", e.getMessage(), e);
    } finally {
      this.rebuildInProgress = false;

      if (context != null) {
        context.getWiki().getStore().cleanUp(context);
      }
    }

    LOGGER.debug("Lucene index rebuild done");
  }

  /**
   * First empties the index, then fetches all Documents, their translations and their
   * attachments for re-addition to the index.
   *
   * @param context
   *          the XWiki context
   * @return the number of indexed elements
   * @throws InterruptedException
   */
  private int rebuildIndex(XWikiContext context) throws InterruptedException {
    int retval = 0;
    Collection<String> wikiServers = this.wikis;
    if (wikiServers == null) {
      XWiki xwiki = context.getWiki();
      if (xwiki.isVirtualMode()) {
        wikiServers = findWikiServers(context);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("found [{}] virtual wikis:", wikiServers.size());
          for (String wikiName : wikiServers) {
            LOGGER.debug(wikiName);
          }
        }
      } else {
        // No virtual wiki configuration, just index the wiki the context belongs to
        wikiServers = new ArrayList<String>();
        wikiServers.add(context.getDatabase());
      }
    }
    // Iterate all found virtual wikis
    for (String wikiName : wikiServers) {
      int wikiResult = indexWiki(wikiName, context);
      if (wikiResult > 0) {
        retval += wikiResult;
      }
    }
    return retval;
  }

  /**
   * Adds the content of a given wiki to the indexUpdater's queue.
   *
   * @param wikiName
   *          the name of the wiki to index
   * @param context
   *          the XWiki context
   * @return the number of indexed elements
   * @throws InterruptedException
   */
  protected int indexWiki(String wikiName, XWikiContext context)
      throws InterruptedException {
    LOGGER.info("Reading content of wiki [{}]", wikiName);
    // Number of index entries processed
    int retval = -1;
    String database = context.getDatabase();
    Searcher searcher = null;
    try {
      context.setDatabase(wikiName);
      searcher = new IndexSearcher(this.indexUpdater.getDirectory(), true);
      retval = indexDocuments(wikiName, searcher, context);
    } catch (IOException exc) {
      LOGGER.error("Failed reading or writing index for wiki [{}]", wikiName, exc);
    } catch (XWikiException xwe) {
      LOGGER.error("Error getting documents for wiki [{}] and filter [{}]", wikiName,
          this.hqlFilter, xwe);
    } finally {
      context.setDatabase(database);
      IOUtils.closeQuietly(searcher);
    }
    return retval;
  }

  private int indexDocuments(String wikiName, Searcher searcher, XWikiContext context)
      throws InterruptedException, IOException, XWikiException {
    int retval = 0;
    Set<String> remainingDocs = getAllIndexedDocs(wikiName, searcher);
    for (Object[] document : getAllDocs(wikiName, context)) {
      DocumentReference docRef = new DocumentReference(wikiName, (String) document[0],
          (String) document[1]);
      String version = (String) document[2];
      String language = (String) document[3];
      if (!this.onlyNew || !isIndexed(docRef, version, language, searcher)) {
        try {
          retval += addTranslationOfDocument(docRef, language, context);
        } catch (XWikiException e) {
          LOGGER.error("Error fetching document [{}] for language [{}]",
              new Object[] { docRef, language, e });
          return retval;
        }
      }
      remainingDocs.remove(getWebUtils().serializeRef(docRef) + "." + language);
    }
    if (this.cleanIndex) {
      cleanIndex(remainingDocs);
    }
    return retval;
  }

  private List<Object[]> getAllDocs(String wikiName, XWikiContext context)
      throws XWikiException {
    String hql = "select distinct doc.space, doc.name, doc.version, doc.language"
        + "from XWikiDocument as doc ";
    if (StringUtils.isNotBlank(this.hqlFilter)) {
      if ((this.hqlFilter.charAt(0) != ',') && !this.hqlFilter.contains("where")
          && !this.hqlFilter.contains("WHERE")) {
        hql += "where ";
      }
      hql += this.hqlFilter;
    }
    return context.getWiki().search(hql, context);
  }

  public Set<String> getAllIndexedDocs(String wikiName, Searcher searcher)
      throws IOException {
    Set<String> ret = new HashSet<>();
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI, wikiName.toLowerCase())),
        BooleanClause.Occur.MUST);
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    TopDocs topDocs = searcher.search(query, Math.max(1, collector.getTotalHits()));
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      ret.add(searcher.doc(scoreDoc.doc).get(IndexFields.DOCUMENT_ID));
    }
    return ret;
  }

  private void cleanIndex(Set<String> danglingDocs) throws IOException {
    IndexWriter writer = null;
    try {
      writer = this.indexUpdater.openWriter(false);
      for (String docId : danglingDocs) {
        writer.deleteDocuments(new Term(IndexFields.DOCUMENT_ID, docId));
      }
    } finally {
      IOUtils.closeQuietly(writer);
    }
  }

  protected int addTranslationOfDocument(DocumentReference documentReference,
      String language, XWikiContext wikiContext)
          throws XWikiException, InterruptedException {
    int retval = 0;

    XWikiDocument document = wikiContext.getWiki().getDocument(documentReference,
        wikiContext);
    XWikiDocument tdocument = document.getTranslatedDocument(language, wikiContext);

    // In order not to load the whole database in memory, we're limiting the number
    // of documents that are in the processing queue at a moment. We could use a
    // Bounded Queue in the index updater, but that would generate exceptions in the
    // rest of the platform, as the index rebuilder could fill the queue, and then a
    // user trying to save a document would cause an exception. Thus, it is better
    // to limit the index rebuilder thread only, and not the index updater.
    while (this.indexUpdater.getQueueSize() > this.indexUpdater.getMaxQueueSize()) {
      // Don't leave any database connections open while sleeping
      // This shouldn't be needed, but we never know what bugs might be there
      wikiContext.getWiki().getStore().cleanUp(wikiContext);
      Thread.sleep(RETRYINTERVAL);
    }

    addTranslationOfDocument(tdocument, wikiContext);
    ++retval;

    if (document == tdocument) {
      retval += this.indexUpdater.queueAttachments(document, wikiContext);
    }

    return retval;
  }

  protected void addTranslationOfDocument(XWikiDocument document,
      XWikiContext wikiContext) {
    this.indexUpdater.queueDocument(document, wikiContext, false);
  }

  private Collection<String> findWikiServers(XWikiContext context) {
    List<String> retval = Collections.emptyList();

    try {
      retval = context.getWiki().getVirtualWikisDatabaseNames(context);

      if (!retval.contains(context.getMainXWiki())) {
        retval.add(context.getMainXWiki());
      }
    } catch (Exception e) {
      LOGGER.error("Error getting list of wiki servers!", e);
    }

    return retval;
  }

  public boolean isIndexed(DocumentReference docRef, Searcher searcher)
      throws IOException {
    return isIndexed(docRef, null, null, searcher);
  }

  public boolean isIndexed(DocumentReference docRef, String version, String language,
      Searcher searcher) throws IOException {
    boolean exists = false;
    BooleanQuery query = new BooleanQuery();
    query.add(
        new TermQuery(
            new Term(IndexFields.DOCUMENT_NAME, docRef.getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    query.add(
        new TermQuery(new Term(IndexFields.DOCUMENT_SPACE,
            docRef.getLastSpaceReference().getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    query.add(
        new TermQuery(new Term(IndexFields.DOCUMENT_WIKI,
            docRef.getWikiReference().getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    if (version != null) {
      query.add(new TermQuery(new Term(IndexFields.DOCUMENT_VERSION, version)),
          BooleanClause.Occur.MUST);
    }
    if (language != null) {
      query.add(
          new TermQuery(new Term(IndexFields.DOCUMENT_LANGUAGE,
              StringUtils.isEmpty(language) ? "default" : language)),
          BooleanClause.Occur.MUST);
    }
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    exists = collector.getTotalHits() > 0;
    return exists;
  }

  private IWebUtilsService getWebUtils() {
    return Utils.getComponent(IWebUtilsService.class);
  }

}
