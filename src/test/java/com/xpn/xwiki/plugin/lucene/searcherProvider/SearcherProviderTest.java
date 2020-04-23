package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static com.celements.common.test.CelementsTestUtils.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.lang.Thread.State;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.search.IndexSearcher;
import org.junit.Before;
import org.junit.Test;

import com.celements.common.test.AbstractComponentTest;
import com.google.common.base.Stopwatch;
import com.xpn.xwiki.plugin.lucene.SearchResults;
import com.xpn.xwiki.plugin.lucene.searcherProvider.SearcherProviderManager.DisconnectToken;

public class SearcherProviderTest extends AbstractComponentTest {

  private IndexSearcher theMockSearcher;
  private DisconnectToken tokenMock;
  private SearcherProvider searcherProvider;

  private ExecutorService threadPool = Executors.newFixedThreadPool(1);

  @Before
  public void setUp_SearcherProviderTest() throws Exception {
    theMockSearcher = createMockAndAddToDefault(IndexSearcher.class);
    tokenMock = createMockAndAddToDefault(DisconnectToken.class);
    List<IndexSearcher> searchers = Arrays.asList(theMockSearcher);
    searcherProvider = new SearcherProvider(searchers, tokenMock);
    expect(tokenMock.isUsed()).andReturn(false).anyTimes();
  }

  @Test
  public void test_SearcherProvider() {
    replayDefault();
    assertNotNull(searcherProvider);
    assertFalse(searcherProvider.isMarkedToClose());
    assertNotNull(searcherProvider.connectedThreads);
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    verifyDefault();
  }

  @Test
  public void test_IsMarkedToClose_markClose() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    replayDefault();
    assertFalse(searcherProvider.isMarkedToClose());
    searcherProvider.markToClose();
    assertTrue(searcherProvider.isMarkedToClose());
    searcherProvider.markToClose();
    assertTrue(searcherProvider.isMarkedToClose());
    verifyDefault();
  }

  @Test
  public void test_Connect_connectAconnectedThreadAfterMarkToClose() throws Exception {
    assertTrue(searcherProvider.connectedThreads.isEmpty());
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
  public void test_Connect() {
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connect();
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedThreads.contains(Thread.currentThread()));
    verifyDefault();
  }

  @Test
  public void test_Connect_onMarkClose_illegalState() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    replayDefault();
    searcherProvider.markToClose();
    try {
      searcherProvider.connect();
      fail("expecting illegal state exception if not connected before calling" + " getSearchers");
    } catch (IllegalStateException exp) {
      // expected
    }
    verifyDefault();
  }

  @Test
  public void test_GetSearchers_illegalState() {
    replayDefault();
    try {
      assertNotNull(searcherProvider.getSearchers());
      assertSame(theMockSearcher, searcherProvider.getSearchers().get(0));
      fail("expecting illegal state exception if not connected before calling" + " getSearchers");
    } catch (IllegalStateException exp) {
      // expected
    }
    verifyDefault();
  }

  @Test
  public void test_GetSearchers() {
    replayDefault();
    searcherProvider.connect();
    assertNotNull(searcherProvider.getSearchers());
    assertSame(theMockSearcher, searcherProvider.getSearchers().get(0));
    verifyDefault();
  }

  @Test
  public void test_Disconnect_withoutClose() throws Exception {
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connectedThreads.add(Thread.currentThread());
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    verifyDefault();
  }

  @Test
  public void test_Disconnect_withClose() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connectedThreads.add(Thread.currentThread());
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    verifyDefault();
  }

  @Test
  public void test_Disconnect_withoutClose_beforeLast() throws Exception {
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    Lock lock = new ReentrantLock();
    lock.lock();
    searcherProvider.connect();
    Thread testThread = new Thread(() -> {
      try {
        Thread.sleep(10000L);
      } catch (InterruptedException e) {}
    });
    testThread.start();
    waitForState(testThread, State.TIMED_WAITING);
    searcherProvider.connectedThreads.add(testThread);
    assertEquals(2, searcherProvider.connectedThreads.size());
    searcherProvider.markToClose();
    assertTrue(testThread.isAlive());
    searcherProvider.disconnect();
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    verifyDefault();
    testThread.interrupt();
  }

  @Test
  public void test_Disconnect_withClose_onLast() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connect();
    final Thread testThread = new Thread();
    searcherProvider.connectedThreads.add(testThread);
    assertEquals(2, searcherProvider.connectedThreads.size());
    searcherProvider.markToClose();
    searcherProvider.connectedThreads.remove(testThread);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    verifyDefault();
  }

  @Test
  public void test_IsIdle_connectedThread() throws Exception {
    replayDefault();
    searcherProvider.connect();
    searcherProvider.markToClose();
    assertTrue(searcherProvider.isMarkedToClose());
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    assertFalse(searcherProvider.isIdle());
    verifyDefault();
  }

  @Test
  public void test_IsIdle_notMarkedClose() throws Exception {
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    assertFalse(searcherProvider.isMarkedToClose());
    assertTrue(searcherProvider.isIdle());
    searcherProvider.tryClose();
    assertFalse(searcherProvider.isClosed());
    verifyDefault();
  }

  @Test
  public void test_IsIdle_connectedResultSet() throws Exception {
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.disconnect();
    searcherProvider.markToClose();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.isMarkedToClose());
    assertFalse(searcherProvider.connectedSearchResults.isEmpty());
    assertFalse(searcherProvider.isIdle());
    verifyDefault();
  }

  @Test
  public void test_IsIdle_true() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    replayDefault();
    searcherProvider.markToClose();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.isMarkedToClose());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    assertTrue(searcherProvider.isIdle());
    verifyDefault();
  }

  @Test
  public void test_ConnectSearchResults_withoutClose() throws Exception {
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertFalse(searcherProvider.isClosed());
    assertFalse(searcherProvider.connectedSearchResults.isEmpty());
    verifyDefault();
  }

  @Test
  public void test_ConnectSearchResults_withClose() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertFalse(searcherProvider.isClosed());
    searcherProvider.cleanUpSearchResults(searchResults);
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    // assertTrue(searcherProvider.isClosed());
    verifyDefault();
  }

  @Test
  public void test_CleanUpAllSearchResultsForThread() throws Exception {
    theMockSearcher.close();
    expectLastCall().once();
    expect(tokenMock.use(same(searcherProvider))).andReturn(true).once();
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    SearchResults searchResults2 = createMockAndAddToDefault(SearchResults.class);
    replayDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.connect();
    searcherProvider.connectSearchResults(searchResults);
    searcherProvider.connectSearchResults(searchResults2);
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    searcherProvider.markToClose();
    searcherProvider.disconnect();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertFalse(searcherProvider.isClosed());
    searcherProvider.cleanUpAllSearchResultsForThread();
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    // assertTrue(searcherProvider.isClosed());
    verifyDefault();
  }

  @Test
  public void test_ConnectSearchResult_illegalState() throws Exception {
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

  @Test
  public void test_tryClose_retainActivThread() throws Exception {
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    Thread testThread = new Thread();
    replayDefault();
    searcherProvider.connectedThreads.add(testThread);
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    testThread.start();
    assertTrue(testThread.isAlive());
    searcherProvider.tryClose();
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    testThread.join();
    assertFalse(testThread.isAlive());
    searcherProvider.tryClose();
    verifyDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
  }

  @Test
  public void test_tryClose_removeOrphanedThread() throws Exception {
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    Thread testThread = new Thread();
    searcherProvider.connectedThreads.add(testThread);
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    replayDefault();
    searcherProvider.tryClose();
    verifyDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
  }

  @Test
  public void test_tryClose_removeOrphanedThread_threadPool() throws Exception {
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    Thread testThread = getWaitingThreadFromPool();
    searcherProvider.connectedThreads.add(testThread);
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    replayDefault();
    searcherProvider.tryClose();
    verifyDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
  }

  @Test
  public void test_tryClose_removeLeftOverSearchResult_connectedOrphanedThread() throws Exception {
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    Thread testThread = getWaitingThreadFromPool();
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    searcherProvider.connectedThreads.add(testThread);
    searcherProvider.connectedSearchResults.putIfAbsent(testThread, new HashSet<SearchResults>());
    searcherProvider.connectedSearchResults.get(testThread).add(searchResults);
    assertFalse(searcherProvider.connectedThreads.isEmpty());
    assertFalse(searcherProvider.connectedSearchResults.isEmpty());
    replayDefault();
    searcherProvider.tryClose();
    verifyDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
  }

  @Test
  public void test_tryClose_removeLeftOverSearchResult_disconnectedOrphanedThread()
      throws Exception {
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
    Thread testThread = getWaitingThreadFromPool();
    SearchResults searchResults = createMockAndAddToDefault(SearchResults.class);
    searcherProvider.connectedSearchResults.putIfAbsent(testThread, new HashSet<SearchResults>());
    searcherProvider.connectedSearchResults.get(testThread).add(searchResults);
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertFalse(searcherProvider.connectedSearchResults.isEmpty());
    replayDefault();
    searcherProvider.tryClose();
    verifyDefault();
    assertTrue(searcherProvider.connectedThreads.isEmpty());
    assertTrue(searcherProvider.connectedSearchResults.isEmpty());
  }

  private Thread getWaitingThreadFromPool()
      throws InterruptedException, ExecutionException {
    AtomicReference<Thread> thread = new AtomicReference<>();
    threadPool.submit(() -> thread.set(Thread.currentThread())).get();
    waitForState(thread.get(), Thread.State.WAITING);
    assertTrue(thread.get().isAlive());
    return thread.get();
  }

  private void waitForState(Thread thread, Thread.State state) throws InterruptedException {
    Stopwatch watch = Stopwatch.createStarted();
    while (thread.getState() != state) {
      Thread.sleep(1);
      if (watch.elapsed().getSeconds() > 5) {
        thread.interrupt();
        throw new RuntimeException("waitForState timed out");
      }
    }
  }

}
