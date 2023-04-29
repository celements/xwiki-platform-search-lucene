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
import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Predicates.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.Requirement;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;

import com.celements.common.date.DateUtil;
import com.celements.model.access.IModelAccessFacade;
import com.celements.model.access.exception.DocumentLoadException;
import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.model.context.Contextualiser;
import com.celements.model.context.ModelContext;
import com.celements.model.metadata.DocumentMetaData;
import com.celements.model.util.ModelUtils;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.celements.search.lucene.index.rebuild.LuceneIndexRebuildService;
import com.celements.store.DocumentCacheStore;
import com.celements.store.MetaDataStoreExtension;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.store.XWikiCacheStoreInterface;
import com.xpn.xwiki.util.AbstractXWikiRunnable;
import com.xpn.xwiki.web.Utils;

import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TLinkedHashSet;

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
@Component
public class IndexRebuilder implements LuceneIndexRebuildService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexRebuilder.class);

  static final String PROP_MAX_QUEUE_SIZE = "xwiki.plugins.lucene.maxQueueSize";
  static final String PROP_PAUSE_DURATION = "xwiki.plugins.lucene.pauseDuration";

  @Requirement
  private IModelAccessFacade modelAccess;

  @Requirement
  private ModelUtils modelUtils;

  @Requirement
  private ModelContext context;

  /**
   * seconds to pause while waiting for the indexing queue to empty.
   */
  private final AtomicLong pauseDuration = new AtomicLong(10);

  /**
   * Soft threshold after which no more documents will be added to the indexing queue.
   * When the queue size gets larger than this value, the index rebuilding thread will
   * sleep chunks of {@code IndexRebuilder#retryInterval} milliseconds until the queue
   * size will get back bellow this threshold. This does not affect normal indexing
   * through wiki updates.
   */
  private final AtomicLong maxQueueSize = new AtomicLong(1000);

  /**
   * The actual object/thread that indexes data.
   */
  private final AtomicReference<IndexUpdater> indexUpdater = new AtomicReference<>();

  private final Executor rebuildExecutor = Executors.newSingleThreadExecutor();
  private final Queue<IndexRebuildFuture> rebuildQueue = new LinkedList<>();
  private final AtomicReference<Instant> paused = new AtomicReference<>(Instant.MIN);

  @Override
  public void initialize(IndexUpdater indexUpdater) {
    checkState(this.indexUpdater.compareAndSet(null, checkNotNull(indexUpdater)),
        "LuceneIndexRebuildService already initialized");
    this.maxQueueSize.set(getXContext().getWiki().ParamAsLong(PROP_MAX_QUEUE_SIZE, 1000));
    this.pauseDuration.set(getXContext().getWiki().ParamAsLong(PROP_PAUSE_DURATION, 10));
    LOGGER.info("LuceneIndexRebuildService initialized");
  }

  private IndexUpdater expectIndexUpdater() {
    IndexUpdater ret = indexUpdater.get();
    checkState(ret != null, "LuceneIndexRebuildService not initialized");
    return ret;
  }

  @Override
  public synchronized Optional<IndexRebuildFuture> getRunningRebuild() {
    return rebuildQueue.stream().filter(not(CompletableFuture::isDone)).findFirst();
  }

  @Override
  public synchronized Optional<IndexRebuildFuture> getQueuedRebuild(EntityReference filterRef) {
    return rebuildQueue.stream().filter(f -> f.getReference().equals(filterRef)).findFirst();
  }

  @Override
  public synchronized ImmutableList<IndexRebuildFuture> getQueuedRebuilds() {
    return ImmutableList.copyOf(rebuildQueue);
  }

  @Override
  public synchronized IndexRebuildFuture startIndexRebuild(final EntityReference filterRef) {
    rebuildQueue.removeIf(CompletableFuture::isDone);
    return getQueuedRebuild(filterRef).orElseGet(() -> {
      IndexRebuildFuture newFuture = new IndexRebuildFuture(filterRef);
      rebuildQueue.add(newFuture);
      try {
        rebuildIndexAsync(newFuture);
      } catch (Exception exc) {
        LOGGER.error("[{}] - failed to run rebuild async", filterRef, exc);
        newFuture.completeExceptionally(exc);
      }
      return newFuture;
    });
  }

  protected void rebuildIndexAsync(final IndexRebuildFuture future) {
    final EntityReference filterRef = future.getReference();
    final Directory directory = expectIndexUpdater().getDirectory();
    final WikiReference wikiRef = filterRef.extractRef(WikiReference.class).get();
    new Contextualiser().withWiki(wikiRef).execute(() -> CompletableFuture.runAsync(
        new AbstractXWikiRunnable(XWikiContext.EXECUTIONCONTEXT_KEY, createJobContext()) {

          @Override
          protected void runInternal() {
            LOGGER.info("[{}] - started", logRef(filterRef));
            try (IndexSearcher searcher = new IndexSearcher(directory, true)) {
              long count = rebuildIndex(searcher, filterRef);
              LOGGER.info("[{}] - finished: {}", logRef(filterRef), count);
              future.complete(count);
            } catch (InterruptedException exc) {
              LOGGER.error("[{}] - interrupted", filterRef, exc);
              future.completeExceptionally(exc);
              Thread.currentThread().interrupt();
            } catch (Exception exc) {
              LOGGER.error("[{}] - failed", filterRef, exc);
              future.completeExceptionally(exc);
            }
          }
        }, rebuildExecutor));
  }

  /**
   * creates a new XWikiContext instance for an async job. use ModelContextBuilder when
   * [CELDEV-534] is complete.
   */
  private XWikiContext createJobContext() {
    XWikiContext ctx = new XWikiContext();
    ctx.setEngineContext(getXContext().getEngineContext());
    ctx.setDatabase(getXContext().getDatabase());
    ctx.setLanguage(getXContext().getLanguage());
    ctx.setMainXWiki(getXContext().getMainXWiki());
    ctx.setWiki(getXContext().getWiki());
    ctx.getWiki().getStore().cleanUp(ctx);
    ctx.flushClassCache();
    ctx.flushArchiveCache();
    return ctx;
  }

  private long rebuildIndex(IndexSearcher searcher, EntityReference filterRef)
      throws IOException, InterruptedException {
    long ret = 0;
    long count = 0;
    Set<DocumentMetaData> docsToIndex = getAllDocMetaData(filterRef);
    Set<String> docsDangling = getAllIndexedDocs(filterRef, searcher);
    int toIndexCount = docsToIndex.size();
    LOGGER.info("[{}] - indexing {} docs with {} dangling",
        logRef(filterRef), toIndexCount, docsDangling.size());
    for (Iterator<DocumentMetaData> iter = docsToIndex.iterator(); iter.hasNext();) {
      DocumentMetaData metaData = iter.next();
      String docId = getDocId(metaData);
      ret += queueDocument(metaData);
      LOGGER.trace("indexed {}", docId);
      iter.remove();
      docsDangling.remove(docId);
      if ((++count % 1000) == 0) {
        LOGGER.info("[{}] - indexed {}/{} with {} dangling",
            logRef(filterRef), count, toIndexCount, docsDangling.size());
      }
    }
    LOGGER.info("[{}] - indexed {} docs with {} dangling",
        logRef(filterRef), count, docsDangling.size());
    cleanIndex(docsDangling);
    return ret;
  }

  private Set<DocumentMetaData> getAllDocMetaData(@NotNull EntityReference ref) {
    MetaDataStoreExtension store;
    if (getXContext().getWiki().getStore() instanceof MetaDataStoreExtension) {
      store = (MetaDataStoreExtension) getXContext().getWiki().getStore();
    } else {
      store = (MetaDataStoreExtension) Utils.getComponent(XWikiCacheStoreInterface.class,
          DocumentCacheStore.COMPONENT_NAME);
    }
    return new TLinkedHashSet<>(store.listDocumentMetaData(ref));
  }

  private Set<String> getAllIndexedDocs(@NotNull EntityReference ref,
      @NotNull IndexSearcher searcher) throws IOException {
    Set<String> ret = new THashSet<>();
    Query query = getLuceneSearchRefQuery(ref);
    TotalHitCountCollector collector = new TotalHitCountCollector();
    searcher.search(query, collector);
    TopDocs topDocs = searcher.search(query, Math.max(1, collector.getTotalHits()));
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      ret.add(searcher.doc(scoreDoc.doc).get(IndexFields.DOCUMENT_ID));
    }
    LOGGER.info("getAllIndexedDocs: [{}] for query [{}]", topDocs.scoreDocs.length, query);
    return ret;
  }

  private void cleanIndex(Set<String> danglingDocs) throws InterruptedException {
    LOGGER.info("cleanIndex for [{}] dangling docs", danglingDocs.size());
    danglingDocs.remove(null);
    danglingDocs.remove("");
    for (String docId : danglingDocs) {
      waitIfPaused();
      queue(new DeleteData(docId));
      LOGGER.trace("cleanIndex {}", docId);
    }
  }

  protected int queueDocument(DocumentMetaData metaData) throws InterruptedException {
    int retval = 0;
    try {
      waitIfPaused();
      XWikiDocument doc = modelAccess.getDocument(metaData.getDocRef(), metaData.getLanguage());
      queue(new DocumentData(doc, false));
      ++retval;
      if (!doc.isTrans()) {
        for (XWikiAttachment att : doc.getAttachmentList()) {
          queue(new AttachmentData(att, false));
          ++retval;
        }
      }
    } catch (DocumentNotExistsException exc) {
      LOGGER.info("unable to queue inexistent doc '{}'", metaData);
    } catch (DocumentLoadException exc) {
      LOGGER.error("failed to queue doc '{}'", metaData, exc);
    }
    return retval;
  }

  private void queue(AbstractIndexData data) {
    data.setPriority(IndexQueuePriority.LOWEST);
    data.setDisableObservationEventNotification(true);
    expectIndexUpdater().queue(data);
  }

  @Override
  public void pause(Duration duration) {
    duration = Optional.ofNullable(duration).orElse(Duration.ZERO);
    Instant until = Instant.now().plus(duration);
    paused.set(until);
    LOGGER.info("paused for {} until {}", duration, until.atZone(DateUtil.getDefaultZone()));
  }

  @Override
  public Optional<Instant> isPaused() {
    return Optional.ofNullable(paused.get()).filter(i -> i.isAfter(Instant.now()));
  }

  private void waitIfPaused() throws InterruptedException {
    pauseIfHighQueueSize();
    while (isPaused().isPresent()) {
      synchronized (paused) {
        // wait must be in synchronized block
        Duration timeout = Duration.between(Instant.now(), paused.get());
        LOGGER.debug("waiting for {}", timeout);
        paused.wait(Math.max(timeout.toMillis(), 1));
        LOGGER.debug("waiting ended");
      }
      pauseIfHighQueueSize();
    }
  }

  @Override
  public void unpause() {
    Instant now = Instant.now();
    if (paused.getAndSet(now).isAfter(now)) {
      synchronized (paused) {
        // notify must be in synchronized block
        paused.notifyAll();
      }
      LOGGER.info("unpaused");
    }
  }

  // In order not to load the whole database in memory, we're limiting the number
  // of documents that are in the processing queue at a moment. We could use a
  // Bounded Queue in the index updater, but that would generate exceptions in the
  // rest of the platform, as the index rebuilder could fill the queue, and then a
  // user trying to save a document would cause an exception. Thus, it is better
  // to limit the index rebuilder thread only, and not the index updater.
  private void pauseIfHighQueueSize() {
    long queueSize = expectIndexUpdater().getQueueSize();
    if (queueSize >= maxQueueSize.get()) {
      // Don't leave any database connections open while sleeping
      // This shouldn't be needed, but we never know what bugs might be there
      getXContext().getWiki().getStore().cleanUp(getXContext());
      LOGGER.info("pausing for {}s since queue size {} too big", pauseDuration.get(), queueSize);
      paused.set(Instant.now().plusSeconds(pauseDuration.get()));
    }
  }

  private BooleanQuery getLuceneSearchRefQuery(@NotNull EntityReference ref) {
    BooleanQuery query = new BooleanQuery();
    ref.extractRef(DocumentReference.class)
        .ifPresent(docRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_NAME_S,
            docRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    ref.extractRef(SpaceReference.class)
        .ifPresent(spaceRef -> query.add(new TermQuery(new Term(IndexFields.DOCUMENT_SPACE_S,
            spaceRef.getName().toLowerCase())), BooleanClause.Occur.MUST));
    WikiReference wikiRef = ref.extractRef(WikiReference.class).get();
    query.add(new TermQuery(new Term(IndexFields.DOCUMENT_WIKI, wikiRef.getName().toLowerCase())),
        BooleanClause.Occur.MUST);
    return query;
  }

  private String getDocId(DocumentMetaData metaData) {
    StringBuilder sb = new StringBuilder();
    sb.append(modelUtils.serializeRef(metaData.getDocRef()));
    sb.append(".");
    if (!Strings.isNullOrEmpty(metaData.getLanguage())) {
      sb.append(metaData.getLanguage());
    } else {
      sb.append("default");
    }
    return sb.toString();
  }

  private final Supplier<String> logRef(EntityReference ref) {
    return defer(() -> modelUtils.serializeRef(ref));
  }

  private final XWikiContext getXContext() {
    return context.getXWikiContext();
  }

}
