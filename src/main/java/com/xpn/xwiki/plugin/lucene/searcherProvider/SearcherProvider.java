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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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
  private Searcher[] searchers;

  private boolean markToClose;

  private Set<Thread> connectedThreads;

  private HashMap<Thread, Set<SearchResults>> connectedSearchResultsMap;

  SearcherProvider(Searcher[] searchers) {
    this.searchers = searchers;
    this.markToClose = false;
    this.connectedThreads = new HashSet<Thread>();
    this.connectedSearchResultsMap = new HashMap<Thread, Set<SearchResults>>();
    LOGGER.debug("create searcherProvider: [" + System.identityHashCode(this) + "].");
  }

  HashMap<Thread, Set<SearchResults>> internal_getConnectedSearchResults() {
    return connectedSearchResultsMap;
  }

  Set<Thread> internal_getConnectedThreads() {
    return connectedThreads;
  }

  /**
   * connect and markToClose need to be externally synchronized
   */
  public void connect() {
    if (!checkConnected()) {
      if (this.markToClose) {
        throw new IllegalStateException("you may not connect to a SearchProvider"
            + " marked to close.");
      }
      LOGGER.debug("connect searcherProvider [" + System.identityHashCode(this) + "] to ["
          + Thread.currentThread() + "].");
      connectedThreads.add(Thread.currentThread());
    }
  }

  public boolean isClosed() {
    return (this.searchers == null);
  }

  public Searcher[] getSearchers() {
    if (!checkConnected()) {
      throw new IllegalStateException("you must connect to the searcher provider before"
          + " you can get any searchers");
    }
    return this.searchers;
  }

  private boolean checkConnected() {
    return this.connectedThreads.contains(Thread.currentThread());
  }

  public void disconnect() throws IOException {
    if (connectedThreads.remove(Thread.currentThread())) {
      LOGGER.debug("disconnect searcherProvider [" + System.identityHashCode(this) + "] to ["
          + Thread.currentThread() + "], markedToClose [" + isMarkedToClose() + "].");
      closeIfIdle();
    }
  }

  public boolean isMarkedToClose() {
    return this.markToClose;
  }

  /**
   * connect and markToClose need to be externally synchronized
   */
  public void markToClose() throws IOException {
    if (!this.markToClose) {
      LOGGER.debug("markToClose searcherProvider [" + System.identityHashCode(this) + "].");
      this.markToClose = true;
      closeIfIdle();
    }
  }

  private void closeIfIdle() throws IOException {
    if (canBeClosed()) {
      closeSearchers();
    }
  }

  boolean canBeClosed() {
    return this.markToClose && isIdle();
  }

  public boolean isIdle() {
    return connectedThreads.isEmpty() && connectedSearchResultsMap.isEmpty();
  }

  /**
   * @throws IOException
   */
  void closeSearchers() throws IOException {
    if (this.searchers != null) {
      LOGGER.debug("closeSearchers: for [" + System.identityHashCode(this) + "].");
      for (int i = 0; i < this.searchers.length; i++) {
        if (this.searchers[i] != null) {
          this.searchers[i].close();
        }
      }
      this.searchers = null;
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
    if (!this.checkConnected()) {
      throw new IllegalStateException("you may not connect a searchResult to a"
          + " SearchProvider from a not connected thread.");
    }
    getConnectedSearchResultsForCurrentThread().add(searchResults);
  }

  Set<SearchResults> getConnectedSearchResultsForCurrentThread() {
    if (!hasSearchResultsForCurrentThread()) {
      connectedSearchResultsMap.put(Thread.currentThread(), new HashSet<SearchResults>());
    }
    return connectedSearchResultsMap.get(Thread.currentThread());
  }

  public boolean hasSearchResultsForCurrentThread() {
    return connectedSearchResultsMap.containsKey(Thread.currentThread());
  }

  public void cleanUpAllSearchResultsForThread() throws IOException {
    if (hasSearchResultsForCurrentThread()) {
      connectedSearchResultsMap.remove(Thread.currentThread());
      closeIfIdle();
    }
  }

  public void cleanUpSearchResults(SearchResults searchResults) throws IOException {
    if (hasSearchResultsForCurrentThread()) {
      Set<SearchResults> currentThreadSet = connectedSearchResultsMap.get(Thread.currentThread());
      if (currentThreadSet.remove(searchResults)) {
        if (currentThreadSet.isEmpty()) {
          connectedSearchResultsMap.remove(Thread.currentThread());
        }
        closeIfIdle();
      }
    }
  }

}
