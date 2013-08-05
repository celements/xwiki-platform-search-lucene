package com.xpn.xwiki.plugin.lucene;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import org.apache.lucene.search.Searcher;
import org.junit.Before;
import org.junit.Test;

import com.celements.common.test.AbstractBridgedComponentTestCase;

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
  public void testConnect() {
    replayDefault();
    assertTrue(searcherProvider.internal_getConnectedThreads().isEmpty());
    searcherProvider.connect();
    assertFalse(searcherProvider.internal_getConnectedThreads().isEmpty());
    assertTrue(searcherProvider.internal_getConnectedThreads().contains(
        Thread.currentThread()));
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
    } catch(IllegalStateException exp) {
      //expected
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

}
