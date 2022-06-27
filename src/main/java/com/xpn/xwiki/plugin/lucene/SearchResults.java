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
import java.util.List;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.model.reference.DocumentReference;

import com.celements.rights.access.EAccessLevel;
import com.celements.rights.access.IRightsAccessFacadeRole;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.api.Api;
import com.xpn.xwiki.api.XWiki;
import com.xpn.xwiki.plugin.lucene.searcherProvider.SearcherProvider;
import com.xpn.xwiki.web.Utils;

/**
 * Container for the results of a search.
 * <p>
 * This class handles paging through search results and enforces the xwiki rights
 * management by only returning search results the user executing the search is allowed to
 * view.
 * </p>
 *
 * @version $Id: 111c8d01d68749e3b4214a49238963ab10b70445 $
 */
public class SearchResults extends Api {

  private final XWiki xwiki;

  private final Searcher searcher;

  private final SearcherProvider searcherProvider;

  private final Query query;

  private final TopDocsCollector<? extends ScoreDoc> results;

  private final boolean skipChecks;

  private static final Logger LOGGER = LoggerFactory.getLogger(SearchResults.class);

  private List<SearchResult> relevantResults;

  private IRightsAccessFacadeRole rightsAccess;

  /**
   * @param results
   *          Lucene search results
   * @param searcher
   * @param skipChecks
   *          skips exists and access checks on documents
   * @param xwiki
   *          xwiki instance for access rights checking
   */
  SearchResults(Query query, TopDocsCollector<? extends ScoreDoc> results, Searcher searcher,
      SearcherProvider theSearcherProvider, boolean skipChecks, XWiki xwiki, XWikiContext context) {
    super(context);
    this.query = query;
    this.results = results;
    this.searcher = searcher;
    this.searcherProvider = theSearcherProvider;
    this.searcherProvider.connectSearchResults(this);
    this.skipChecks = skipChecks;
    this.xwiki = xwiki;
  }

  private List<SearchResult> getRelevantResults() {
    return getRelevantResults(null, null);
  }

  private List<SearchResult> getRelevantResults(Integer offset, Integer limit) {
    if (this.relevantResults == null) {
      this.relevantResults = new ArrayList<>();
      try {
        TopDocs docs = getTopDocs(offset, limit);
        LOGGER.debug("getRelevantResults: checking access to scoreDocs [{}] for results ["
            + "{}] with class [{}] and id-Hash [{}].", docs.scoreDocs.length,
            results.getTotalHits(), results.getClass(), System.identityHashCode(results));
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
          try {
            SearchResult result = new SearchResult(searcher.doc(scoreDoc.doc), scoreDoc.score,
                this.xwiki);
            if (result.isWikiContent()) {
              try {
                if (skipChecks || check(result.getDocumentReference())) {
                  this.relevantResults.add(result);
                } else {
                  LOGGER.debug("getRelevantResults: skipping because checks failed for result {}].",
                      result.getDocumentReference());
                }
              } catch (XWikiException xwe) {
                LOGGER.error("Error checking result: {}", result.getFullName(), xwe);
              }
            } else {
              LOGGER.debug("getRelevantResults: skipping because no wiki content"
                  + " (wiki-Document or wiki-Doc-Attachment).");
            }
          } catch (IOException ioe) {
            LOGGER.error("Error getting result doc '{}' from searcher", scoreDoc, ioe);
          }
        }
      } finally {
        try {
          searcherProvider.cleanUpSearchResults(this);
        } catch (IOException exp) {
          LOGGER.error("Failed to cleanUpSearchResults on searchProvider.", exp);
        }
      }
    } else {
      LOGGER.debug("getRelevantResults: returning cached relevantResults [{}].",
          relevantResults.size());
    }

    return this.relevantResults;
  }

  private TopDocs getTopDocs(Integer offset, Integer limit) {
    final int start = (offset != null) ? offset : 0;
    if (limit != null) {
      return this.results.topDocs(start, limit);
    } else {
      return this.results.topDocs(start);
    }
  }

  private boolean check(DocumentReference docRef) throws XWikiException {
    return xwiki.exists(docRef) && getRightsAccess().hasAccessLevel(docRef, EAccessLevel.VIEW);
  }

  /**
   * @return true when there are more results than currently displayed.
   */
  public boolean hasNext(String beginIndex, String items) {
    final int itemCount = Integer.parseInt(items);
    final int begin = Integer.parseInt(beginIndex);

    return ((begin + itemCount) - 1) < getRelevantResults().size();
  }

  /**
   * @return true when there is a page before the one currently displayed, that is, when
   *         <code>beginIndex > 1</code>
   */
  public boolean hasPrevious(String beginIndex) {
    return Integer.parseInt(beginIndex) > 1;
  }

  /**
   * @return the value to be used for the firstIndex URL parameter to build a link
   *         pointing to the next page of results
   */
  public int getNextIndex(String beginIndex, String items) {
    final int itemCount = Integer.parseInt(items);
    final int resultcount = getRelevantResults().size();
    int retval = Integer.parseInt(beginIndex) + itemCount;

    return retval > resultcount ? ((resultcount - itemCount) + 1) : retval;
  }

  /**
   * @return the value to be used for the firstIndex URL parameter to build a link
   *         pointing to the previous page of results
   */
  public int getPreviousIndex(String beginIndex, String items) {
    int retval = Integer.parseInt(beginIndex) - Integer.parseInt(items);

    return 0 < retval ? retval : 1;
  }

  /**
   * @return the index of the last displayed search result
   */
  public int getEndIndex(String beginIndex, String items) {
    int retval = (Integer.parseInt(beginIndex) + Integer.parseInt(items)) - 1;
    final int resultcount = getRelevantResults().size();
    if (retval > resultcount) {
      return resultcount;
    }

    return retval;
  }

  /**
   * Helper method for use in velocity templates, takes string values instead of ints. See
   * {@link #getResults(int,int)}.
   */
  public List<SearchResult> getResults(String beginIndex, String items) {
    return getResults(Integer.parseInt(beginIndex), Integer.parseInt(items));
  }

  /**
   * Returns a list of search results. According to beginIndex and endIndex, only a subset
   * of the results is returned. To get the first ten results, one would use beginIndex=1
   * and items=10.
   *
   * @param beginIndex
   *          1-based index of first result to return.
   * @param items
   *          number of items to return
   * @return List of SearchResult instances starting at <code>beginIndex</code> and
   *         containing up to <code>items</code> elements.
   */
  public List<SearchResult> getResults(int beginIndex, int items) {
    int listStartIndex = Math.max(beginIndex - 1, 0);
    return getRelevantResults(listStartIndex, items);
  }

  /**
   * @return all search results in one list.
   */
  public List<SearchResult> getResults() {
    return getRelevantResults();
  }

  /**
   * @return total number of searchresults the user is allowed to view
   */
  public int getHitcount() {
    return getRelevantResults().size();
  }

  /**
   * @return total number of searchresults including unallowed items
   */
  public int getTotalHitcount() {
    return this.results.getTotalHits();
  }

  @Override
  protected void finalize() throws Throwable {
    LOGGER.debug("finalize SearchResults for [{}], isMarkedClose [{}], searcherProvider.isClosed[{"
        + "}], isIdle [{}].", System.identityHashCode(this), searcherProvider.isMarkedToClose(),
        searcherProvider.isClosed(), searcherProvider.isIdle());
    searcherProvider.cleanUpSearchResults(this);
    LOGGER.debug("finalize SearchResults for [{}], isMarkedClose [{}], searcherProvider.isClosed [{"
        + "}], isIdle [{}].", System.identityHashCode(this), searcherProvider.isMarkedToClose(),
        searcherProvider.isClosed(), searcherProvider.isIdle());
  }

  private IRightsAccessFacadeRole getRightsAccess() {
    if (rightsAccess == null) {
      rightsAccess = Utils.getComponent(IRightsAccessFacadeRole.class);
    }
    return rightsAccess;
  }

  @Override
  public String toString() {
    return "SearchResults [query=" + query + ", hitcount=" + getTotalHitcount() + "]";
  }

}
