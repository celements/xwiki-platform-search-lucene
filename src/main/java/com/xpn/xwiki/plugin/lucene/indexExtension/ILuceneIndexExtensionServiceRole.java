package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.Collection;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.xwiki.component.annotation.ComponentRole;

import com.celements.search.lucene.index.IndexData;
import com.xpn.xwiki.plugin.lucene.indexExtension.IndexExtensionField.ExtensionType;

@ComponentRole
public interface ILuceneIndexExtensionServiceRole {

  public void extend(IndexData data, Document luceneDoc);

  public IndexExtensionField createField(String name, String value, Index indexType,
      ExtensionType extensionType);

  public IndexExtensionField createField(String name, Number value, ExtensionType extensionType)
      throws IllegalArgumentException;

  public Collection<IndexExtensionField> createFields(String name, Object value,
      ExtensionType defaultExtType) throws IllegalArgumentException;

  public Collection<IndexExtensionField> createFields(Map<String, Object> fieldMap,
      ExtensionType defaultExtType) throws IllegalArgumentException;

  public IndexExtensionField createRemoveField(String name);

}
