package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static com.celements.common.test.CelementsTestUtils.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.xwiki.observation.EventListener;
import org.xwiki.observation.event.ActionExecutionEvent;
import org.xwiki.observation.event.Event;

import com.celements.common.test.AbstractComponentTest;
import com.xpn.xwiki.web.Utils;

public class ActionExecutionEventListenerTest extends AbstractComponentTest {

  private ActionExecutionEventListener listener;
  private ISearcherProviderRole searcherProviderManager;

  @Before
  public void preprareTest() throws Exception {
    searcherProviderManager = registerComponentMock(ISearcherProviderRole.class);
    listener = (ActionExecutionEventListener) Utils.getComponent(EventListener.class,
        "SearcherProviderManager");
  }

  @Test
  public void test_singletonComponent() {
    assertSame(listener, Utils.getComponent(EventListener.class, "SearcherProviderManager"));
  }

  @Test
  public void test_getEvents() {
    List<Event> expectedEventsList = Arrays.<Event>asList(
        new ActionExecutionEvent("view"),
        new ActionExecutionEvent("inline"),
        new ActionExecutionEvent("edit"),
        new ActionExecutionEvent("admin"),
        new ActionExecutionEvent("import"));
    replayDefault();
    assertEquals(expectedEventsList, listener.getEvents());
    verifyDefault();
  }

  @Test
  public void test_onEvent_empty() {
    searcherProviderManager.closeAllForCurrentThread();
    expectLastCall().once();
    replayDefault();
    listener.onEvent(new ActionExecutionEvent("view"), null, getContext());
    verifyDefault();
  }

}
