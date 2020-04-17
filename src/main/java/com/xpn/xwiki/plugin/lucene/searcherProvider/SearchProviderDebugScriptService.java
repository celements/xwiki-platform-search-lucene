package com.xpn.xwiki.plugin.lucene.searcherProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.Requirement;
import org.xwiki.script.service.ScriptService;

import com.celements.rights.access.IRightsAccessFacadeRole;

@Component("searchProviderDebug")
public class SearchProviderDebugScriptService implements ScriptService {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SearchProviderDebugScriptService.class);

  @Requirement
  private IRightsAccessFacadeRole rightsAccess;

  @Requirement
  private ISearcherProviderRole searchProvider;

  public void logState() {
    if (rightsAccess.isSuperAdmin()) {
      searchProvider.logState(LOGGER);
    }
  }

}
