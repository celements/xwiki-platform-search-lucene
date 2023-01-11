package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.Collection;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.xwiki.component.annotation.ComponentRole;

import com.xpn.xwiki.plugin.lucene.AbstractIndexData;
import com.xpn.xwiki.plugin.lucene.indexExtension.IndexExtensionField.ExtensionType;

@ComponentRole
public interface ILuceneIndexExtensionServiceRole {

  void extend(AbstractIndexData data, Document luceneDoc);

  void extend(IndexExtensionField field, Document luceneDoc);

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField.Builder}
   */
  @Deprecated
  IndexExtensionField createField(String name, String value, Index indexType,
      ExtensionType extensionType);

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField.Builder}
   */
  @Deprecated
  IndexExtensionField createField(String name, Number value, ExtensionType extensionType)
      throws IllegalArgumentException;

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField#createFromValue}
   */
  @Deprecated
  Collection<IndexExtensionField> createFields(String name, Object value,
      ExtensionType defaultExtType) throws IllegalArgumentException;

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField#createFromMap}
   */
  @Deprecated
  Collection<IndexExtensionField> createFields(Map<String, Object> fieldMap,
      ExtensionType defaultExtType) throws IllegalArgumentException;

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField#createRemove}
   */
  @Deprecated
  IndexExtensionField createRemoveField(String name);

}
