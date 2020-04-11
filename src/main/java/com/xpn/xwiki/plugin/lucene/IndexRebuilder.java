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

import static com.celements.logging.LogUtils.*;
import static com.google.common.base.MoreObjects.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;

import com.celements.model.access.ContextExecutor;
import com.celements.model.access.IModelAccessFacade;
import com.celements.model.access.exception.DocumentLoadException;
import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.model.context.ModelContext;
import com.celements.model.metadata.DocumentMetaData;
import com.celements.model.util.ModelUtils;
import com.celements.model.util.References;
import com.celements.store.DocumentCacheStore;
import com.celements.store.MetaDataStoreExtension;
import com.google.common.base.Strings;
import com.xpn.xwiki.XWikiContext;
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
public class IndexRebuilder {

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

  private final Executor rebuildExecutor = Executors.newSingleThreadExecutor();
  private final AtomicReference<CompletableFuture<Long>> latestFuture = new AtomicReference<>();

  public IndexRebuilder(IndexUpdater indexUpdater) {
    this.indexUpdater = indexUpdater;
    this.retryInterval = 1000 * (int) getContext().getXWikiContext().getWiki()
        .ParamAsLong(PROP_UPDATER_RETRY_INTERVAL, 30);
    this.maxQueueSize = getContext().getXWikiContext().getWiki()
        .ParamAsLong(PROP_MAX_QUEUE_SIZE, 1000);
  }

  public Optional<CompletableFuture<Long>> getLatestRebuildFuture() {
    return Optional.ofNullable(latestFuture.get());
  }

  public CompletableFuture<Long> startIndexRebuild(EntityReference entityRef) {
    final EntityReference filterRef = References.cloneRef(
        firstNonNull(entityRef, getContext().getWikiRef()));
    WikiReference wikiRef = References.extractRef(filterRef, WikiReference.class).get();
    return ContextExecutor.executeInWiki(wikiRef, () -> rebuildIndexAsync(filterRef));
  }

  protected CompletableFuture<Long> rebuildIndexAsync(final EntityReference filterRef) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    CompletableFuture.runAsync(new AbstractXWikiRunnable(
        XWikiContext.EXECUTIONCONTEXT_KEY, getContext().getXWikiContext().clone()) {

      @Override
      protected void runInternal() {
        LOGGER.info("Lucene index rebuild started for [{}]", logRef(filterRef));
        try (IndexSearcher searcher = new IndexSearcher(indexUpdater.getDirectory(), true)) {
          long count = rebuildIndex(searcher, filterRef);
          LOGGER.info("Lucene index rebuild finished for [{}]: {}", logRef(filterRef), count);
          future.complete(count);
        } catch (InterruptedException exc) {
          future.completeExceptionally(exc);
          Thread.currentThread().interrupt();
        } catch (Exception exc) {
          LOGGER.error("Error in lucene rebuild thread: {}", exc.getMessage(), exc);
          future.completeExceptionally(exc);
        }
      }
    }, rebuildExecutor);
    latestFuture.set(future);
    return future;
  }

  private long rebuildIndex(IndexSearcher searcher, EntityReference filterRef)
      throws IOException, InterruptedException {
    long ret = 0;
    long count = 0;
    Set<String> docsInIndex = getAllIndexedDocs(filterRef, searcher);
    Set<DocumentMetaData> docsToIndex = getAllDocMetaData(filterRef);
    for (DocumentMetaData metaData : docsToIndex) {
      String docId = getDocId(metaData);
      ret += queueDocument(metaData);
      LOGGER.trace("indexed {}", docId);
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

  private Set<DocumentMetaData> getAllDocMetaData(@NotNull EntityReference filterRef) {
    MetaDataStoreExtension store;
    if (getContext().getXWikiContext().getWiki().getStore() instanceof MetaDataStoreExtension) {
      store = (MetaDataStoreExtension) getContext().getXWikiContext().getWiki().getStore();
    } else {
      store = (MetaDataStoreExtension) Utils.getComponent(XWikiCacheStoreInterface.class,
          DocumentCacheStore.COMPONENT_NAME);
    }
    return store.listDocumentMetaData(filterRef);
  }

  public Set<String> getAllIndexedDocs(@NotNull EntityReference ref,
      @NotNull IndexSearcher searcher) throws IOException {
    Set<String> ret = new HashSet<>();
    Query query = getLuceneSearchRefQuery(ref);
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    TopDocs topDocs = searcher.search(query, Math.max(1, collector.getTotalHits()));
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      ret.add(searcher.doc(scoreDoc.doc).get(IndexFields.DOCUMENT_ID));
    }
    LOGGER.info("getAllIndexedDocs: found {} docs in index for ref '{}'", ret.size(), logRef(ref));
    return ret;
  }

  private void cleanIndex(Set<String> danglingDocs) throws InterruptedException {
    LOGGER.info("cleanIndex: for {} dangling docs", danglingDocs.size());
    danglingDocs.remove(null);
    danglingDocs.remove("");
    for (String docId : danglingDocs) {
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
      LOGGER.info("unable to queue inexistent doc '{}'", metaData);
    } catch (DocumentLoadException exc) {
      LOGGER.error("failed to queue doc '{}': {}", metaData, exc);
    }
    return retval;
  }

  private void queue(AbstractIndexData data) {
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

  private BooleanQuery getLuceneSearchRefQuery(@NotNull EntityReference ref) {
    BooleanQuery query = new BooleanQuery();
    References.extractRef(ref, DocumentReference.class).toJavaUtil()
        .ifPresent(docRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_NAME,
            docRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    References.extractRef(ref, SpaceReference.class).toJavaUtil()
        .ifPresent(spaceRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_SPACE,
            spaceRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    WikiReference wikiRef = References.extractRef(ref, WikiReference.class).get();
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI, wikiRef.getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    return query;
  }

  static String getDocId(DocumentMetaData metaData) {
    StringBuilder sb = new StringBuilder();
    sb.append(getModelUtils().serializeRef(metaData.getDocRef()));
    sb.append(".");
    if (!Strings.isNullOrEmpty(metaData.getLanguage())) {
      sb.append(metaData.getLanguage());
    } else {
      sb.append("default");
    }
    return sb.toString();
  }

  private final Supplier<String> logRef(EntityReference ref) {
    return defer(() -> getModelUtils().serializeRef(ref));
  }

  private static IModelAccessFacade getModelAccess() {
    return Utils.getComponent(IModelAccessFacade.class);
  }

  private static ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

  private static ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

}
