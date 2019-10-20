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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.validation.constraints.NotNull;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.suigeneris.jrcs.rcs.Version;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;

import com.celements.model.access.IModelAccessFacade;
import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.model.context.ModelContext;
import com.celements.model.metadata.DocumentMetaData;
import com.celements.model.util.ModelUtils;
import com.celements.model.util.References;
import com.celements.search.lucene.index.AttachmentData;
import com.celements.search.lucene.index.DeleteData;
import com.celements.search.lucene.index.DocumentData;
import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.LuceneDocId;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.celements.search.lucene.index.queue.IndexQueuePriorityManager;
import com.celements.search.lucene.index.queue.LuceneIndexingQueue;
import com.celements.store.DocumentCacheStore;
import com.celements.store.MetaDataStoreExtension;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.xpn.xwiki.XWiki;
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

  private final Directory directory;

  private final LuceneIndexingQueue indexingQueue;

  /**
   * Variable used for indicating that a rebuild is already in progress.
   */
  private final AtomicBoolean rebuildInProgress = new AtomicBoolean(false);

  /**
   * Wikis to rebuild
   */
  private volatile Set<WikiReference> wikis = null;

  /**
   * Reference to filter reindex data with.
   */
  private volatile Optional<EntityReference> filterRef = Optional.empty();

  /**
   * Indicate if document already in the Lucene index are updated.
   */
  private volatile boolean onlyNew = false;

  /**
   * Indicate if the Lucene index should be wiped, if false index will be cleaned from inexistent
   * documents.
   */
  private volatile boolean wipeIndex = false;

  public IndexRebuilder(Directory directory) {
    super(XWikiContext.EXECUTIONCONTEXT_KEY, getContext().getXWikiContext().clone());
    this.directory = directory;
    this.indexingQueue = Utils.getComponent(LuceneIndexingQueue.class);
  }

  public boolean startIndexRebuildWithWipe(List<WikiReference> wikis, boolean onlyNew) {
    this.wipeIndex = true;
    return startIndexRebuild(wikis, Optional.empty(), onlyNew);
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

  private Set<WikiReference> getWikis(List<WikiReference> wikis) {
    ImmutableSet.Builder<WikiReference> builder = new ImmutableSet.Builder<>();
    if (wikis != null) {
      builder.addAll(wikis);
    } else {
      builder.add(getContext().getWikiRef());
      if (getWiki().isVirtualMode()) {
        builder.add(getModelUtils().getMainWikiRef());
        try {
          getWiki().getVirtualWikisDatabaseNames(getContext().getXWikiContext()).stream()
              .map(WikiReference::new)
              .forEach(builder::add);
        } catch (XWikiException xwe) {
          LOGGER.error("failed to load virtual wiki names", xwe);
        }
      }
    }
    return builder.build();
  }

  @Override
  protected void runInternal() {
    LOGGER.info("Starting lucene index rebuild");
    XWikiContext context = null;
    try {
      // TODO [CELDEV-543] IndexUpdater/Rebuilder XWikiContext clone
      // The context must be cloned, as otherwise setDatabase() might affect the response to the
      // current request. This is not a good way to do this; ideally there would be a method that
      // creates a new context and copies only a few needed objects, as some objects are not
      // supposed to be used in 2 different contexts.
      // This seems to work on a simple run:
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

      getIndexQueuePriorityManager().putPriority(getRebuldingPriority());

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
   */
  private void rebuildIndex() {
    WikiReference beforeWikiRef = getContext().getWikiRef();
    for (WikiReference wikiRef : wikis) {
      getContext().setWikiRef(wikiRef);
      try (IndexSearcher searcher = new IndexSearcher(directory, true)) {
        rebuildWiki(wikiRef, searcher);
      } catch (IOException | InterruptedException exc) {
        LOGGER.error("Failed rebulding wiki [{}]", wikiRef, exc);
      } finally {
        getContext().setWikiRef(beforeWikiRef);
      }
    }
  }

  private void rebuildWiki(@NotNull WikiReference wikiRef, @NotNull IndexSearcher searcher)
      throws IOException, InterruptedException {
    EntityReference ref = this.filterRef.orElse(checkNotNull(wikiRef));
    checkArgument(References.extractRef(ref, WikiReference.class).get().equals(wikiRef),
        "unable to index wiki '" + wikiRef + "' for set filter '" + ref + "'");
    LOGGER.info("rebuilding wiki '{}'", wikiRef);
    if (wipeIndex) {
      wipeWikiIndex(ref);
    }
    Set<LuceneDocId> indexed = rebuildIndex(ref, searcher);
    if (!wipeIndex) {
      cleanIndex(Sets.difference(getAllIndexedDocs(ref, searcher), indexed));
    }
  }

  public Set<LuceneDocId> rebuildIndex(EntityReference ref, IndexSearcher searcher)
      throws IOException, InterruptedException {
    Set<LuceneDocId> indexed = new HashSet<>();
    Set<DocumentMetaData> docsToIndex = getAllDocMetaData(ref);
    int totalSize = docsToIndex.size();
    for (Iterator<DocumentMetaData> iter = docsToIndex.iterator(); iter.hasNext();) {
      DocumentMetaData metaData = iter.next();
      LuceneDocId docId = getDocId(metaData);
      if (!onlyNew || !isIndexed(metaData, searcher)) {
        queueDocument(metaData);
        LOGGER.trace("indexed {}", docId);
      } else {
        LOGGER.trace("skipped '{}', already indexed", docId);
      }
      indexed.add(docId);
      if ((indexed.size() % 500) == 0) {
        LOGGER.info("indexed docs {}/{}", indexed.size(), totalSize);
      }
      iter.remove(); // reduces docsToIndex and thus the memory footprint
    }
    return indexed;
  }

  private Set<DocumentMetaData> getAllDocMetaData(@NotNull EntityReference ref) {
    MetaDataStoreExtension store;
    if (getWiki().getStore() instanceof MetaDataStoreExtension) {
      store = (MetaDataStoreExtension) getWiki().getStore();
    } else {
      store = (MetaDataStoreExtension) Utils.getComponent(XWikiCacheStoreInterface.class,
          DocumentCacheStore.COMPONENT_NAME);
    }
    return store.listDocumentMetaData(ref);
  }

  private Set<LuceneDocId> getAllIndexedDocs(@NotNull EntityReference ref,
      @NotNull IndexSearcher searcher) throws IOException {
    Set<LuceneDocId> ret = new HashSet<>();
    Query query = getLuceneSearchRefQuery(ref);
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    TopDocs topDocs = searcher.search(query, Math.max(1, collector.getTotalHits()));
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      try {
        ret.add(LuceneDocId.parse(searcher.doc(scoreDoc.doc).get(IndexFields.DOCUMENT_ID)));
      } catch (IllegalArgumentException iae) {
        LOGGER.warn("encountered invalid doc in index: {}", scoreDoc, iae);
      }
    }
    LOGGER.info("getAllIndexedDocs: found {} docs in index for ref '{}'", ret.size(), ref);
    return ret;
  }

  private void wipeWikiIndex(@NotNull EntityReference ref) throws InterruptedException {
    WikiReference wikiRef = References.extractRef(ref, WikiReference.class).get();
    LOGGER.info("wipeWikiIndex: for '{}'", wikiRef);
    queue(new DeleteData(new LuceneDocId(wikiRef)));
  }

  private void cleanIndex(Set<LuceneDocId> danglingDocs) throws InterruptedException {
    LOGGER.info("cleanIndex: for {} dangling docs", danglingDocs.size());
    danglingDocs.remove(null);
    for (LuceneDocId docId : danglingDocs) {
      queue(new DeleteData(docId));
      LOGGER.trace("cleanIndex: deleted doc: {}", docId);
    }
  }

  protected int queueDocument(DocumentMetaData metaData) throws InterruptedException {
    int retval = 0;
    try {
      XWikiDocument doc = getModelAccess().getDocument(metaData.getDocRef(),
          metaData.getLanguage());
      queue(new DocumentData(doc));
      ++retval;
      if (!getModelAccess().isTranslation(doc)) {
        for (XWikiAttachment att : doc.getAttachmentList()) {
          queue(new AttachmentData(att));
          ++retval;
        }
      }
    } catch (DocumentNotExistsException exc) {
      LOGGER.warn("failed to queue doc '{}'", metaData);
    }
    return retval;
  }

  private void queue(IndexData data) throws InterruptedException {
    data.disableObservationEventNotification();
    data.setPriority(getRebuldingPriority());
    indexingQueue.put(data);
  }

  private IndexQueuePriority getRebuldingPriority() {
    return IndexQueuePriority.LOW;
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
    final BooleanQuery query = new BooleanQuery();
    References.extractRef(ref, DocumentReference.class).toJavaUtil()
        .ifPresent(docRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_NAME,
            docRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    References.extractRef(ref, SpaceReference.class).toJavaUtil()
        .ifPresent(spaceRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_SPACE,
            spaceRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    References.extractRef(ref, WikiReference.class).toJavaUtil()
        .ifPresent(wikiRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI,
            wikiRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    return query;
  }

  private static XWiki getWiki() {
    return getContext().getXWikiContext().getWiki();
  }

  static LuceneDocId getDocId(DocumentMetaData metaData) {
    return new LuceneDocId(metaData.getDocRef(), metaData.getLanguage());
  }

  private static IndexQueuePriorityManager getIndexQueuePriorityManager() {
    return Utils.getComponent(IndexQueuePriorityManager.class);
  }

  private static IModelAccessFacade getModelAccess() {
    return Utils.getComponent(IModelAccessFacade.class);
  }

  private static ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

  private static ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

}
