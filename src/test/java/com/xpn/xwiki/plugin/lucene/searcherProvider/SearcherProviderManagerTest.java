package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static org.easymock.EasyMock.*;

import static junit.framework.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.lucene.search.Searcher;
import org.junit.Before;
import org.junit.Test;
import org.xwiki.observation.event.ActionExecutionEvent;
import org.xwiki.observation.event.Event;

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
  public void testGetEvents() {
    List<Event> expectedEventsList = Arrays.asList((Event)new ActionExecutionEvent("view"
        ), (Event)new ActionExecutionEvent("edit"), (Event)new ActionExecutionEvent(
            "admin"), (Event)new ActionExecutionEvent("import"));
    replayDefault();
    assertEquals(expectedEventsList, theSearchProvManager.getEvents());
    verifyDefault();
  }

  @Test
  public void testGetAllSearcherProvider() {
    replayDefault();
    Vector<SearcherProvider> searcherProvList =
        theSearchProvManager.getAllSearcherProvider();
    assertNotNull(searcherProvList);
    assertSame(searcherProvList, theSearchProvManager.getAllSearcherProvider());
    verifyDefault();
  }

  @Test
  public void testCreateSearchProvider() {
    Searcher theMockSearcher = createMockAndAddToDefault(Searcher.class);
    replayDefault();
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    SearcherProvider searcherProv =
        theSearchProvManager.createSearchProvider(new Searcher[] {theMockSearcher});
    assertNotNull(searcherProv);
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    verifyDefault();
  }

  @Test
  public void testOnEvent_empty() {
    replayDefault();
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    theSearchProvManager.onEvent(new ActionExecutionEvent("view"), null, context);
    verifyDefault();
  }

  @Test
  public void testOnEvent_notEmpty_notMarkedClosed() {
    Searcher theMockSearcher = createMockAndAddToDefault(Searcher.class);
    SearchResults mockSearchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    SearcherProvider searcherProv =
        theSearchProvManager.createSearchProvider(new Searcher[] {theMockSearcher});
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    searcherProv.connect();
    searcherProv.connectSearchResults(mockSearchResults);
    assertTrue(searcherProv.hasSearchResultsForCurrentThread());
    theSearchProvManager.onEvent(new ActionExecutionEvent("view"), null, context);
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
    SearcherProvider searcherProv =
        theSearchProvManager.createSearchProvider(new Searcher[] {theMockSearcher});
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    searcherProv.connect();
    searcherProv.markToClose();
    searcherProv.connectSearchResults(mockSearchResults);
    theSearchProvManager.onEvent(new ActionExecutionEvent("view"), null, context);
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
    SearcherProvider searcherProv =
        theSearchProvManager.createSearchProvider(new Searcher[] {theMockSearcher});
    assertEquals(1, theSearchProvManager.getAllSearcherProvider().size());
    searcherProv.connect();
    searcherProv.markToClose();
    searcherProv.connectSearchResults(mockSearchResults);
    searcherProv.disconnect();
    theSearchProvManager.onEvent(new ActionExecutionEvent("view"), null, context);
    assertTrue(theSearchProvManager.getAllSearcherProvider().isEmpty());
    assertFalse(searcherProv.hasSearchResultsForCurrentThread());
    verifyDefault();
  }

}
