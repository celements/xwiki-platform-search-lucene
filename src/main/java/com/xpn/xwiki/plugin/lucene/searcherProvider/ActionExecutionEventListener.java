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

import java.util.List;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.Requirement;
import org.xwiki.observation.EventListener;
import org.xwiki.observation.event.ActionExecutionEvent;
import org.xwiki.observation.event.Event;

import com.google.common.collect.ImmutableList;

@Component("SearcherProviderManager")
@Singleton
public class ActionExecutionEventListener implements EventListener {

  @Requirement
  private ISearcherProviderRole searchProviderManager;

  private static final Logger LOGGER = LoggerFactory.getLogger(ActionExecutionEventListener.class);

  @Override
  public String getName() {
    return "SearcherProviderManager";
  }

  @Override
  public List<Event> getEvents() {
    return ImmutableList.of(
        new ActionExecutionEvent("view"),
        new ActionExecutionEvent("inline"),
        new ActionExecutionEvent("edit"),
        new ActionExecutionEvent("admin"),
        new ActionExecutionEvent("import"));
  }

  @Override
  public void onEvent(Event event, Object source, Object data) {
    LOGGER.trace("onEvent called for event [" + event + "], source [" + source + "], data [" + data
        + "].");
    searchProviderManager.closeAllForCurrentThread();
  }

}
