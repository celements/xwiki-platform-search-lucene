package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Singleton;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;

import com.celements.web.plugin.cmd.PlainTextCommand;
import com.google.common.base.Strings;
import com.xpn.xwiki.plugin.lucene.AbstractIndexData;
import com.xpn.xwiki.plugin.lucene.IndexFields;
import com.xpn.xwiki.plugin.lucene.indexExtension.IndexExtensionField.ExtensionType;
import com.xpn.xwiki.web.Utils;

@Component
@Singleton
public class LuceneIndexExtensionService implements ILuceneIndexExtensionServiceRole {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexExtensionService.class);

  // do not inject since an extender might use this service
  private List<ILuceneIndexExtender> extenders;

  private PlainTextCommand plainTextCmd = new PlainTextCommand();

  private List<ILuceneIndexExtender> getExtenders() {
    if (extenders == null) {
      extenders = Utils.getComponentList(ILuceneIndexExtender.class);
    }
    return extenders;
  }

  @Override
  public void extend(AbstractIndexData data, Document luceneDoc) {
    for (ILuceneIndexExtender ext : getExtenders()) {
      try {
        if (ext.isEligibleIndexData(data)) {
          for (IndexExtensionField extField : ext.getExtensionFields(data)) {
            if (extField != null) {
              switch (extField.getExtensionType()) {
                case ADD:
                  luceneDoc.add(extField.getLuceneField());
                  break;
                case REPLACE:
                  luceneDoc.removeFields(extField.getName());
                  luceneDoc.add(extField.getLuceneField());
                  break;
                case REMOVE:
                  luceneDoc.removeFields(extField.getName());
                  break;
              }
              LOGGER.debug("extendend field '{}' by extender '{}' for data '{}' ", extField,
                  ext.getName(), data);
            }
          }
        } else {
          LOGGER.debug("extend: not eligible extender '{}' for data '{}' ", ext.getName(), data);
        }
      } catch (Exception exc) {
        LOGGER.error("Failed to extend lucene index for '{}'", ext.getName(), exc);
      }
    }
  }

  @Override
  public IndexExtensionField createField(String name, String value, Index indexType,
      ExtensionType extensionType) {
    value = plainTextCmd.convertToPlainText(value).toLowerCase();
    return new IndexExtensionField(extensionType, new Field(name, value, Field.Store.YES,
        indexType));
  }

  @Override
  public IndexExtensionField createField(String name, Number value, ExtensionType extensionType)
      throws IllegalArgumentException {
    return createField(name, IndexFields.numberToString(value), Index.NOT_ANALYZED, extensionType);
  }

  @Override
  public Collection<IndexExtensionField> createFields(String name, Object value,
      ExtensionType defaultExtType) throws IllegalArgumentException {
    Objects.requireNonNull(Strings.emptyToNull(name));
    value = (value != null ? value : "");
    Collection<IndexExtensionField> ret = new ArrayList<>();
    if (value instanceof Collection) {
      Iterator<?> iter = ((Collection<?>) value).iterator();
      while (iter.hasNext()) {
        Object nextVal = iter.next();
        if (nextVal != null) {
          ret.addAll(createFields(name, nextVal, ExtensionType.ADD));
        }
      }
    } else if (value instanceof Number) {
      ret.add(createField(name, (Number) value, defaultExtType));
    } else if (value instanceof Date) {
      ret.add(createField(name, IndexFields.dateToString((Date) value), Index.NOT_ANALYZED,
          defaultExtType));
    } else {
      ret.add(createField(name, value.toString(), Index.ANALYZED, defaultExtType));
    }
    return ret;
  }

  @Override
  public Collection<IndexExtensionField> createFields(Map<String, Object> fieldMap,
      ExtensionType defaultExtType) throws IllegalArgumentException {
    Collection<IndexExtensionField> ret = new ArrayList<>();
    for (String name : fieldMap.keySet()) {
      if (Strings.emptyToNull(name) != null) {
        Object value = fieldMap.get(name);
        if (value != null) {
          ret.addAll(createFields(name, value, defaultExtType));
        }
      }
    }
    return ret;
  }

  @Override
  public IndexExtensionField createRemoveField(String name) {
    return createField(name, "", Index.NOT_ANALYZED, ExtensionType.REMOVE);
  }

}
