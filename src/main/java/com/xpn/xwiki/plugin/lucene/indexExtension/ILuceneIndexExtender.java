package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.Collection;

import org.apache.lucene.document.Fieldable;
import org.xwiki.component.annotation.ComponentRole;

import com.xpn.xwiki.plugin.lucene.AbstractIndexData;

@ComponentRole
public interface ILuceneIndexExtender {

  public boolean isEligibleIndexData(AbstractIndexData data);

  public Collection<Fieldable> getExtensionFields(AbstractIndexData data);

}
