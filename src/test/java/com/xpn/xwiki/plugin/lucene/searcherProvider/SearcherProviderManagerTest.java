package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static junit.framework.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.Vector;

import org.apache.lucene.search.Searcher;
import org.junit.Before;
import org.junit.Test;

import com.celements.common.test.AbstractBridgedComponentTestCase;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.plugin.lucene.SearchResults;
import com.xpn.xwiki.web.Utils;

public class SearcherProviderManagerTest extends AbstractBridgedComponentTestCase {

  private XWikiContext context;
  private SearcherProviderManager theSearchProvManager;

  @Before
  public void setUp_SearcherProviderManagerTest() throws Exception {
    context = getContext();
    theSearchProvManager = (SearcherProviderManager) Utils.getComponent(
        ISearcherProviderRole.class);
  }

  @Test
  public void testSingletonComponent() {
    assertSame(theSearchProvManager, Utils.getComponent(ISearcherProviderRole.class));
  }

  @Test
  public void testGetAllSearcherProvider() {
    replayDefault();
    Vector<SearcherProvider> searcherProvList = theSearchProvManager.getAllSearcherProvider();
    assertNotNull(searcherProvList);
    assertSame(searcherProvList, theSearchProvManager.getAllSearcherProvider());
    verifyDefault();
  }

  @Test
  public void testCreateSearchProvider() {
    Searcher theMockSearcher = createMockAndAddToDefault(Searcher.class);
    replayDefault();
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    SearcherProvider searcherProv = theSearchProvManager.createSearchProvider(new Searcher[] {
        theMockSearcher });
    assertNotNull(searcherProv);
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    verifyDefault();
  }

  @Test
  public void testOnEvent_empty() {
    replayDefault();
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    theSearchProvManager.closeAllForCurrentThread();
    verifyDefault();
  }

  @Test
  public void testOnEvent_notEmpty_notMarkedClosed() {
    Searcher theMockSearcher = createMockAndAddToDefault(Searcher.class);
    SearchResults mockSearchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    SearcherProvider searcherProv = theSearchProvManager.createSearchProvider(new Searcher[] {
        theMockSearcher });
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    searcherProv.connect();
    searcherProv.connectSearchResults(mockSearchResults);
    assertTrue(searcherProv.hasSearchResultsForCurrentThread());
    theSearchProvManager.closeAllForCurrentThread();
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    assertFalse(searcherProv.hasSearchResultsForCurrentThread());
    assertTrue(searcherProv.isIdle());
    verifyDefault();
  }

  @Test
  public void testOnEvent_markedClosed_forgotenDisconnect() throws Exception {
    Searcher theMockSearcher = createMockAndAddToDefault(Searcher.class);
    SearchResults mockSearchResults = createMockAndAddToDefault(SearchResults.class);
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    SearcherProvider searcherProv = theSearchProvManager.createSearchProvider(new Searcher[] {
        theMockSearcher });
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    searcherProv.connect();
    searcherProv.markToClose();
    searcherProv.connectSearchResults(mockSearchResults);
    theSearchProvManager.closeAllForCurrentThread();
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    assertFalse(searcherProv.hasSearchResultsForCurrentThread());
    verifyDefault();
  }

  @Test
  public void testOnEvent_empty_markedClosed() throws Exception {
    Searcher theMockSearcher = createMockAndAddToDefault(Searcher.class);
    SearchResults mockSearchResults = createMockAndAddToDefault(SearchResults.class);
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    SearcherProvider searcherProv = theSearchProvManager.createSearchProvider(new Searcher[] {
        theMockSearcher });
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    searcherProv.connect();
    searcherProv.markToClose();
    searcherProv.connectSearchResults(mockSearchResults);
    searcherProv.disconnect();
    theSearchProvManager.closeAllForCurrentThread();
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    assertFalse(searcherProv.hasSearchResultsForCurrentThread());
    verifyDefault();
  }

}
