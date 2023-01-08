package com.xpn.xwiki.plugin.lucene.indexExtension;

import static java.util.stream.Collectors.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import javax.inject.Singleton;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;

import com.google.common.base.Suppliers;
import com.xpn.xwiki.plugin.lucene.AbstractIndexData;
import com.xpn.xwiki.plugin.lucene.indexExtension.IndexExtensionField.ExtensionType;
import com.xpn.xwiki.web.Utils;

@Component
@Singleton
public class LuceneIndexExtensionService implements ILuceneIndexExtensionServiceRole {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexExtensionService.class);

  // do not inject since an extender might use this service
  private final Supplier<List<ILuceneIndexExtender>> extenders = Suppliers
      .memoize(() -> Utils.getComponentList(ILuceneIndexExtender.class));

  @Override
  public void extend(AbstractIndexData data, Document luceneDoc) {
    extenders.get().stream().forEach(ext -> extend(ext, data, luceneDoc));
  }

  private void extend(ILuceneIndexExtender ext, AbstractIndexData data, Document luceneDoc) {
    try {
      if (ext.isEligibleIndexData(data)) {
        LOGGER.debug("extend: [{}] for [{}]", ext.getName(), data);
        ext.getExtensionFields(data).stream()
            .filter(Objects::nonNull)
            .forEach(field -> extend(field, luceneDoc));
      }
    } catch (Exception exc) {
      LOGGER.error("Failed to extend lucene index with [{}] for [{}]", ext.getName(), data, exc);
    }
  }

  @Override
  public void extend(IndexExtensionField field, Document luceneDoc) {
    switch (field.getExtensionType()) {
      case ADD:
        luceneDoc.add(field.getLuceneField());
        break;
      case REPLACE:
        luceneDoc.removeFields(field.getName());
        luceneDoc.add(field.getLuceneField());
        break;
      case REMOVE:
        luceneDoc.removeFields(field.getName());
        break;
    }
  }

  @Deprecated
  @Override
  public IndexExtensionField createField(String name, String value, Index indexType,
      ExtensionType extensionType) {
    return new IndexExtensionField.Builder(name)
        .extensionType(extensionType)
        .index(indexType)
        .value(value)
        .build();
  }

  @Deprecated
  @Override
  public IndexExtensionField createField(String name, Number value, ExtensionType extensionType)
      throws IllegalArgumentException {
    return new IndexExtensionField.Builder(name)
        .extensionType(extensionType)
        .value(value)
        .build();
  }

  @Deprecated
  @Override
  public Collection<IndexExtensionField> createFields(String name, Object value,
      ExtensionType defaultExtType) throws IllegalArgumentException {
    return IndexExtensionField.createFromValue(name, value, defaultExtType).collect(toList());
  }

  @Deprecated
  @Override
  public Collection<IndexExtensionField> createFields(Map<String, Object> fieldMap,
      ExtensionType defaultExtType) throws IllegalArgumentException {
    return IndexExtensionField.createFromMap(fieldMap, defaultExtType).collect(toList());
  }

  @Deprecated
  @Override
  public IndexExtensionField createRemoveField(String name) {
    return IndexExtensionField.createRemove(name);
  }
}
