package com.xpn.xwiki.plugin.lucene;

import com.google.common.base.Preconditions;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.doc.XWikiDocument;

public class DeleteData extends AbstractIndexData {

  private final String docId;

  public DeleteData(String docId) {
    super("", null, true);
    this.docId = Preconditions.checkNotNull(docId);
  }

  @Override
  public String getId() {
    return docId;
  }

  @Override
  protected void getFullText(StringBuilder sb, XWikiDocument doc, XWikiContext context) {
    // nothing to do
  }

}
