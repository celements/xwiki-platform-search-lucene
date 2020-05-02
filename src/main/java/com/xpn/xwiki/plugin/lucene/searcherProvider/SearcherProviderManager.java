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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;

@Component
@Singleton
public class SearcherProviderManager implements ISearcherProviderRole {

  private static final Logger LOGGER = LoggerFactory.getLogger(SearcherProviderManager.class);

  private Set<SearcherProvider> allSearcherProviderSet = ConcurrentHashMap.newKeySet();

  @Override
  public void closeAllForCurrentThread() {
    int numSearchProviders = getAllSearcherProviders().size();
    LOGGER.debug("closeAllForCurrentThread - start with {} remaining searchProviders",
        numSearchProviders);
    for (Iterator<SearcherProvider> iter = getAllSearcherProviders().iterator(); iter.hasNext();) {
      SearcherProvider searcherProvider = iter.next();
      try {
        searcherProvider.disconnect();
        searcherProvider.cleanUpAllSearchResultsForThread();
      } catch (IOException exp) {
        LOGGER.error("Failed to disconnect searcherProvider from thread.", exp);
      }
      if (searcherProvider.isClosed()) {
        iter.remove();
      }
    }
    LOGGER.debug("closeAllForCurrentThread - finish with {} remaining, {} removed",
        getAllSearcherProviders().size(), (numSearchProviders - getAllSearcherProviders().size()));
  }

  Set<SearcherProvider> getAllSearcherProviders() {
    return allSearcherProviderSet;
  }

  private boolean removeSearchProvider(SearcherProvider searchProvider) {
    boolean ret = getAllSearcherProviders().remove(searchProvider);
    LOGGER.debug("removeSearchProvider - {}: {}, {} remaining", searchProvider, ret,
        getAllSearcherProviders().size());
    return ret;
  }

  /*
   * createSearchProvider must be synchronized to ensure that all threads will see a fully
   * initialized SearcherProvider object
   */
  @Override
  public synchronized SearcherProvider createSearchProvider(List<IndexSearcher> theSearchers) {
    SearcherProvider newSearcherProvider = new SearcherProvider(theSearchers,
        new DisconnectToken());
    getAllSearcherProviders().add(newSearcherProvider);
    LOGGER.debug("createSearchProvider - {}", newSearcherProvider);
    if (getAllSearcherProviders().size() > 20) {
      LOGGER.warn("createSearchProvider - list increased to {}", getAllSearcherProviders().size());
    }
    return newSearcherProvider;
  }

  @Override
  public void logState(Logger log) {
    List<SearcherProvider> providersToClose = getAllSearcherProviders().stream()
        .filter(SearcherProvider::isMarkedToClose).collect(Collectors.toList());
    log.info("logState - {} open search providers marked to close", providersToClose.size());
    providersToClose.forEach(searchProvider -> searchProvider.logState(log));
  }

  class DisconnectToken {

    private final AtomicBoolean used = new AtomicBoolean(false);

    boolean isUsed() {
      return used.get();
    }

    boolean use(SearcherProvider searchProvider) {
      if (used.compareAndSet(false, true)) {
        return removeSearchProvider(searchProvider);
      }
      return false;
    }
  }

}
