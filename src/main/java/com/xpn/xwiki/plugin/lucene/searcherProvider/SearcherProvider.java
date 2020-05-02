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
package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static com.google.common.base.Preconditions.*;
import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.xpn.xwiki.plugin.lucene.SearchResults;
import com.xpn.xwiki.plugin.lucene.searcherProvider.SearcherProviderManager.DisconnectToken;

public class SearcherProvider implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SearcherProvider.class);

  /**
   * List of Lucene indexes used for searching. By default there is only one such index
   * for all the wiki. One searches is created for each entry in {@link #indexDirs}.
   */
  private final List<IndexSearcher> backedSearchers;

  private final DisconnectToken token;

  final Set<Thread> connectedThreads = ConcurrentHashMap.newKeySet();

  final ConcurrentMap<Thread, Set<SearchResults>> connectedSearchResults = new ConcurrentHashMap<>();

  private final AtomicBoolean markToClose = new AtomicBoolean(false);

  SearcherProvider(List<IndexSearcher> searchers, DisconnectToken token) {
    this.backedSearchers = searchers;
    this.token = token;
    LOGGER.debug("create {}", this);
  }

  /**
   * <code>connect</code> is implemented with a fail-fast behavior. The guarantee is, that the
   * SearcherProvider will not close the connected lucene searchers before not all threads
   * finished with theirs SearcherResults and disconnected AND that no thread can connect after
   * marking a SearchProvider for closing.
   */
  public SearcherProvider connect() {
    if (!checkConnected()) {
      synchronized (this) {
        checkState(!isMarkedToClose(), "provider already marked to close");
        LOGGER.debug("connect {} to [{}]", this, getThreadKey());
        connectedThreads.add(getThreadKey());
      }
    }
    return this;
  }

  public boolean isClosed() {
    return token.isUsed();
  }

  public List<IndexSearcher> getSearchers() {
    checkState(!isClosed(), "provider already closed");
    checkState(checkConnected(), "calling thread not connected");
    return backedSearchers;
  }

  private boolean checkConnected() {
    return connectedThreads.contains(getThreadKey());
  }

  public void disconnect() throws IOException {
    if (connectedThreads.remove(getThreadKey())) {
      LOGGER.debug("disconnect {} to [{}]", this, getThreadKey());
      tryClose();
    }
  }

  @Override
  public void close() throws IOException {
    disconnect();
  }

  public boolean isMarkedToClose() {
    return markToClose.get();
  }

  public void markToClose() throws IOException {
    if (markToClose.compareAndSet(false, true)) {
      LOGGER.debug("markToClose {}", this);
      tryClose();
    }
  }

  /**
   * close if is marked and idle, else does nothing
   */
  synchronized void tryClose() throws IOException {
    removeOrphanedThreads();
    if (isMarkedToClose() && isIdle()) {
      closeSearchers();
    }
  }

  private synchronized void removeOrphanedThreads() {
    long size = connectedThreads.size();
    if (connectedThreads.removeIf(thread -> !isRunning(thread))) {
      LOGGER.warn("cleanOrphanedThreads - {} connected threads removed",
          (size - connectedThreads.size()));
    }
    size = getConnectedSearchResultCount();
    if (connectedSearchResults.keySet().removeIf(thread -> !isRunning(thread))) {
      LOGGER.warn("cleanOrphanedThreads - {} connected search results removed",
          (size - getConnectedSearchResultCount()));
    }
  }

  private static final Set<Thread.State> RUNNING_STATES = ImmutableSet.of(
      Thread.State.RUNNABLE, Thread.State.BLOCKED, Thread.State.TIMED_WAITING);

  private boolean isRunning(Thread thread) {
    boolean running = thread.isAlive() && RUNNING_STATES.contains(thread.getState());
    if (!running) {
      LOGGER.info("thread [{}] not running in state [{}]", thread, thread.getState());
    }
    return running;
  }

  private long getConnectedSearchResultCount() {
    return connectedSearchResults.values().stream().flatMap(Set::stream)
        .collect(Collectors.counting());
  }

  public boolean isIdle() {
    return connectedThreads.isEmpty() && connectedSearchResults.isEmpty();
  }

  /**
   * @throws IOException
   */
  void closeSearchers() throws IOException {
    if (!isClosed()) {
      LOGGER.debug("closeSearchers for {}", this);
      for (IndexSearcher searcher : backedSearchers) {
        if (searcher != null) {
          searcher.close();
        }
      }
      token.use(this);
    }
  }

  public void connectSearchResults(SearchResults searchResults) {
    checkState(checkConnected(), "searchResult may not be connected for an unconnected thread");
    getConnectedSearchResultsForCurrentThread().add(searchResults);
  }

  Set<SearchResults> getConnectedSearchResultsForCurrentThread() {
    return connectedSearchResults.computeIfAbsent(getThreadKey(),
        key -> ConcurrentHashMap.newKeySet(1));
  }

  public boolean hasSearchResultsForCurrentThread() {
    return connectedSearchResults.containsKey(getThreadKey());
  }

  public void cleanUpAllSearchResultsForThread() throws IOException {
    if (connectedSearchResults.remove(getThreadKey()) != null) {
      tryClose();
    }
  }

  private Thread getThreadKey() {
    return Thread.currentThread();
  }

  public void cleanUpSearchResults(SearchResults searchResults) throws IOException {
    if (hasSearchResultsForCurrentThread()) {
      Set<SearchResults> currentThreadSet = getConnectedSearchResultsForCurrentThread();
      if (currentThreadSet.remove(searchResults)) {
        if (currentThreadSet.isEmpty()) {
          connectedSearchResults.remove(getThreadKey());
        }
        tryClose();
      }
    }
  }

  public void logState(Logger log) {
    log.info("logState - {}", this);
    log.info("logState - {} connected threads: {}", connectedThreads.size(), connectedThreads);
    log.info("logState - {} connected search results for {} threads",
        connectedSearchResults.values().stream().flatMap(Collection::stream).collect(counting()),
        connectedSearchResults.size());
    connectedSearchResults.forEach((thread, searchResults) -> log.info(
        "logState - {} has {} connected search results: {}",
        thread, searchResults.size(), searchResults));
  }

  @Override
  public String toString() {
    return "SearcherProvider [" + System.identityHashCode(this) + ", isIdle=" + isIdle()
        + ", isMarkedToClose=" + isMarkedToClose() + ", isClosed=" + isClosed() + "]";
  }

}
