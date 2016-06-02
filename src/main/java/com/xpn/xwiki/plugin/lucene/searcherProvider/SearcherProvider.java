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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.search.Searcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xpn.xwiki.plugin.lucene.SearchResults;

public class SearcherProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SearcherProvider.class);

  /**
   * List of Lucene indexes used for searching. By default there is only one such index
   * for all the wiki. One searches is created for each entry in {@link #indexDirs}.
   */
  private final Searcher[] backedSearchers;

  private volatile boolean markToClose;

  private volatile boolean isClosed;

  private Set<Thread> connectedThreads;

  private ConcurrentMap<Thread, Set<SearchResults>> connectedSearchResultsMap;

  SearcherProvider(Searcher[] searchers) {
    this.backedSearchers = searchers;
    this.markToClose = false;
    this.connectedThreads = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());
    this.connectedSearchResultsMap = new ConcurrentHashMap<Thread, Set<SearchResults>>();
    LOGGER.debug("create searcherProvider: [" + System.identityHashCode(this) + "].");
  }

  ConcurrentMap<Thread, Set<SearchResults>> internal_getConnectedSearchResults() {
    return connectedSearchResultsMap;
  }

  Set<Thread> internal_getConnectedThreads() {
    return connectedThreads;
  }

  public void connect() {
    if (!checkConnected()) {
      checkState(!markToClose, "you may not connect to a SearchProvider marked to close.");
      LOGGER.debug("connect searcherProvider [{}] to [{}].", System.identityHashCode(this),
          Thread.currentThread());
      connectedThreads.add(Thread.currentThread());
    }
  }

  public boolean isClosed() {
    return this.isClosed;
  }

  public Searcher[] getSearchers() {
    checkState(!isClosed(), "Getting serachers failed: provider is closed.");
    checkState(checkConnected(), "you must connect to the searcher provider before you can get"
        + " any searchers");
    return backedSearchers;
  }

  private boolean checkConnected() {
    return connectedThreads.contains(Thread.currentThread());
  }

  public void disconnect() throws IOException {
    if (connectedThreads.remove(Thread.currentThread())) {
      LOGGER.debug("disconnect searcherProvider [{}] to [{}], markedToClose [{}].",
          System.identityHashCode(this), Thread.currentThread(), isMarkedToClose());
      closeIfIdle();
    }
  }

  public boolean isMarkedToClose() {
    return markToClose;
  }

  public void markToClose() throws IOException {
    if (!isMarkedToClose()) {
      LOGGER.debug("markToClose searcherProvider [{}].", System.identityHashCode(this));
      markToClose = true;
      closeIfIdle();
    }
  }

  private void closeIfIdle() throws IOException {
    if (canBeClosed()) {
      closeSearchers();
    }
  }

  boolean canBeClosed() {
    return isMarkedToClose() && isIdle();
  }

  public boolean isIdle() {
    return connectedThreads.isEmpty() && connectedSearchResultsMap.isEmpty();
  }

  /**
   * @throws IOException
   */
  synchronized void closeSearchers() throws IOException {
    if (!isClosed()) {
      LOGGER.debug("closeSearchers: for [{}].", System.identityHashCode(this));
      for (Searcher searcher : backedSearchers) {
        if (searcher != null) {
          searcher.close();
        }
      }
      isClosed = true;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!isClosed()) {
      closeSearchers();
    }
  }

  public void connectSearchResults(SearchResults searchResults) {
    checkState(checkConnected(), "you may not connect a searchResult to a SearchProvider from"
        + " a not connected thread.");
    getConnectedSearchResultsForCurrentThread().add(searchResults);
  }

  Set<SearchResults> getConnectedSearchResultsForCurrentThread() {
    connectedSearchResultsMap.putIfAbsent(Thread.currentThread(), Collections.newSetFromMap(
        new ConcurrentHashMap<SearchResults, Boolean>()));
    return connectedSearchResultsMap.get(Thread.currentThread());
  }

  public boolean hasSearchResultsForCurrentThread() {
    return connectedSearchResultsMap.containsKey(Thread.currentThread());
  }

  public void cleanUpAllSearchResultsForThread() throws IOException {
    if (connectedSearchResultsMap.remove(Thread.currentThread()) != null) {
      closeIfIdle();
    }
  }

  public void cleanUpSearchResults(SearchResults searchResults) throws IOException {
    if (hasSearchResultsForCurrentThread()) {
      Set<SearchResults> currentThreadSet = getConnectedSearchResultsForCurrentThread();
      if (currentThreadSet.remove(searchResults)) {
        if (currentThreadSet.isEmpty()) {
          connectedSearchResultsMap.remove(Thread.currentThread());
        }
        closeIfIdle();
      }
    }
  }

}
