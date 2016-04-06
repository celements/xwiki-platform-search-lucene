package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import org.apache.lucene.search.Searcher;
import org.junit.Before;
import org.junit.Test;

import com.celements.common.test.AbstractBridgedComponentTestCase;
import com.xpn.xwiki.plugin.lucene.SearchResults;

public class SearcherProviderTest extends AbstractBridgedComponentTestCase {

  private Searcher theMockSearcher;
  private SearcherProvider searcherProvider;

  @Before
  public void setUp_SearcherProviderTest() throws Exception {
    theMockSearcher = createMockAndAddToDefault(Searcher.class);
    Searcher[] searchers = new Searcher[] { theMockSearcher };
    searcherProvider = new SearcherProvider(searchers);
  }

  @Test
  public void testSearcherProvider() {
    replayDefault();
    assertNotNull(searcherProvider);
    assertFalse(searcherProvider.isMarkedToClose());
    assertNotNull(searcherProvider.internal_getConnectedThreads());
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    verifyDefault();
  }

  @Test
  public void testIsMarkedToClose_markClose() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    assertFalse(searcherProvider.isMarkedToClose());
    searcherProvider.markToClose();
    assertTrue(searcherProvider.isMarkedToClose());
    searcherProvider.markToClose();
    assertTrue(searcherProvider.isMarkedToClose());
    verifyDefault();
  }

  @Test
  public void testConnect_connectAconnectedThreadAfterMarkToClose() throws Exception {
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    searcherProvider.markToClose();
    replayDefault();
    try {
      searcherProvider.connect();
    } catch (IllegalStateException exp) {
      fail("connect may not throw an IllegalStateException on if the thread is already"
          + " connected");
    }
    verifyDefault();
  }

  @Test
  public void testConnect() {
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    assertFalse(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertTrue(
        searcherProvider.internal_getConnectedThreads().contains(Thread.currentThread()));
    verifyDefault();
  }

  @Test
  public void testConnect_onMarkClose_illegalState() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    searcherProvider.markToClose();
    try {
      searcherProvider.connect();
      fail("expecting illegal state exception if not connected before calling"
          + " getSearchers");
    } catch (IllegalStateException exp) {
      // expected
    }
    verifyDefault();
  }

  @Test
  public void testGetSearchers_illegalState() {
    replayDefault();
    try {
      assertNotNull(searcherProvider.getSearchers());
      assertSame(theMockSearcher, searcherProvider.getSearchers()[0]);
      fail("expecting illegal state exception if not connected before calling"
          + " getSearchers");
    } catch (IllegalStateException exp) {
      // expected
    }
    verifyDefault();
  }

  @Test
  public void testGetSearchers() {
    replayDefault();
    searcherProvider.connect();
    assertNotNull(searcherProvider.getSearchers());
    assertSame(theMockSearcher, searcherProvider.getSearchers()[0]);
    verifyDefault();
  }

  @Test
  public void testDisconnect_withoutClose() throws Exception {
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.internal_getConnectedThreads().add(Thread.currentThread());
    assertFalse(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    verifyDefault();
  }

  @Test
  public void testDisconnect_withClose() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.internal_getConnectedThreads().add(Thread.currentThread());
    assertFalse(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    verifyDefault();
  }

  @Test
  public void testDisconnect_withoutClose_beforeLast() throws Exception {
    Thread mockThread = createMockAndAddToDefault(Thread.class);
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    searcherProvider.internal_getConnectedThreads().add(mockThread);
    assertEquals(2, searcherProvider.internal_getConnectedThreads().size());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertFalse(searcherProvider.internal_getConnectedThreads().isEmpty());
    verifyDefault();
  }

  @Test
  public void testDisconnect_withClose_onLast() throws Exception {
    Thread mockThread = createMockAndAddToDefault(Thread.class);
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    searcherProvider.internal_getConnectedThreads().add(mockThread);
    assertEquals(2, searcherProvider.internal_getConnectedThreads().size());
    searcherProvider.markToClose();
    searcherProvider.internal_getConnectedThreads().remove(mockThread);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    verifyDefault();
  }

  @Test
  public void testIsIdle_connectedThread() throws Exception {
    replayDefault();
    searcherProvider.connect();
    searcherProvider.markToClose();
    assertTrue(searcherProvider.isMarkedToClose());
    assertFalse(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertTrue(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    assertFalse(searcherProvider.canBeClosed());
    verifyDefault();
  }

  @Test
  public void testIsIdle_notMarkedClose() throws Exception {
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertTrue(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    assertFalse(searcherProvider.isMarkedToClose());
    assertFalse(searcherProvider.canBeClosed());
    verifyDefault();
  }

  @Test
  public void testIsIdle_connectedResultSet() throws Exception {
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.disconnect();
    searcherProvider.markToClose();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertTrue(searcherProvider.isMarkedToClose());
    assertFalse(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    assertFalse(searcherProvider.canBeClosed());
    verifyDefault();
  }

  @Test
  public void testIsIdle_true() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    replayDefault();
    searcherProvider.markToClose();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertTrue(searcherProvider.isMarkedToClose());
    assertTrue(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    assertTrue(searcherProvider.canBeClosed());
    verifyDefault();
  }

  @Test
  public void testConnectSearchResults_withoutClose() throws Exception {
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertFalse(searcherProvider.isClosed());
    assertFalse(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    verifyDefault();
  }

  @Test
  public void testConnectSearchResults_withClose() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertFalse(searcherProvider.isClosed());
    searcherProvider.cleanUpSearchResults(searchResults);
    assertTrue(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    assertTrue(searcherProvider.isClosed());
    verifyDefault();
  }

  @Test
  public void testCleanUpAllSearchResultsForThread() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    SearchResults searchResults2 = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.connectSearchResults(searchResults2);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertFalse(searcherProvider.isClosed());
    searcherProvider.cleanUpAllSearchResultsForThread();
    assertTrue(searcherProvider.internal_getConnectedSearchResults().isEmpty());
    assertTrue(searcherProvider.isClosed());
    verifyDefault();
  }

  @Test
  public void testConnectSearchResult_illegalState() throws Exception {
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    try {
      searcherProvider.connectSearchResults(searchResults);
      fail("expecting illegal state exception if thread is not connected before calling"
          + " connectSearchResults");
    } catch (IllegalStateException exp) {
      // expected
    }
    verifyDefault();
  }
}
