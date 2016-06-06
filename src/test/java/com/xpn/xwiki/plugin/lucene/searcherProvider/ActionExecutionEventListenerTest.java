package com.xpn.xwiki.plugin.lucene.searcherProvider;

import static junit.framework.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xwiki.observation.EventListener;
import org.xwiki.observation.event.ActionExecutionEvent;
import org.xwiki.observation.event.Event;

import com.celements.common.test.AbstractBridgedComponentTestCase;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.web.Utils;

public class ActionExecutionEventListenerTest extends AbstractBridgedComponentTestCase {

  private XWikiContext context;
  private ActionExecutionEventListener theActionExecListener;
  private ISearcherProviderRole searcherProviderManager;
  private ISearcherProviderRole storeSearchProviderManager;

  @Before
  public void setUp_ActionExecutionEventListener() throws Exception {
    context = getContext();
    theActionExecListener = (ActionExecutionEventListener) Utils.getComponent(EventListener.class,
        "SearcherProviderManager");
    searcherProviderManager = createMockAndAddToDefault(ISearcherProviderRole.class);
    storeSearchProviderManager = theActionExecListener.searchProviderManager;
    theActionExecListener.searchProviderManager = searcherProviderManager;
  }

  @After
  public void tearDown_ActionExecutionEventListener() {
    theActionExecListener.searchProviderManager = storeSearchProviderManager;
  }

  @Test
  public void testSingletonComponent() {
    assertSame(theActionExecListener, Utils.getComponent(EventListener.class,
        "SearcherProviderManager"));
  }

  @Test
  public void testGetEvents() {
    List<Event> expectedEventsList = Arrays.asList((Event) new ActionExecutionEvent("view"),
        (Event) new ActionExecutionEvent("edit"), (Event) new ActionExecutionEvent("admin"),
        (Event) new ActionExecutionEvent("import"));
    replayDefault();
    assertEquals(expectedEventsList, theActionExecListener.getEvents());
    verifyDefault();
  }

  @Test
  public void testOnEvent_empty() {
    searcherProviderManager.closeAllForCurrentThread();
    expectLastCall().once();
    replayDefault();
    theActionExecListener.onEvent(new ActionExecutionEvent("view"), null, context);
    verifyDefault();
  }

}
