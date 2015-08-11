package com.xpn.xwiki.plugin.lucene.indexExtension;

import org.apache.lucene.document.Document;
import org.xwiki.component.annotation.ComponentRole;

import com.xpn.xwiki.plugin.lucene.AbstractIndexData;

@ComponentRole
public interface ILuceneIndexExtensionServiceRole {

  public void extend(AbstractIndexData data, Document luceneDoc);

}
