package com.xpn.xwiki.plugin.lucene.indexExtension;

import static com.celements.common.date.DateFormat.*;
import static com.google.common.base.Strings.*;
import static java.util.Objects.*;

import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;

import com.celements.web.plugin.cmd.ConvertToPlainTextException;
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
              LOGGER.trace("extendend field '{}' by extender '{}' for data '{}' ", extField,
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
    try {
      value = Strings.nullToEmpty(value);
      value = plainTextCmd.convertHtmlToPlainText(value).toLowerCase();
    } catch (ConvertToPlainTextException exc) {
      LOGGER.warn("createField: failed converting value plaintext for field [{}]", name, exc);
    }
    indexType = Optional.ofNullable(indexType).orElseGet(() -> guessIndexType(name));
    return new IndexExtensionField(extensionType, new Field(name, value, Field.Store.YES,
        indexType));
  }

  private Index guessIndexType(String name) {
    name = name.toLowerCase();
    if (name.endsWith("_s") ||
        name.endsWith("_fullname") ||
        StringUtils.isNumeric(name)) {
      return Index.NOT_ANALYZED;
    }
    return Index.ANALYZED;
  }

  @Override
  public IndexExtensionField createField(String name, Number value, ExtensionType extensionType)
      throws IllegalArgumentException {
    return createField(name, IndexFields.numberToString(value), Index.NOT_ANALYZED, extensionType);
  }

  @Override
  public Collection<IndexExtensionField> createFields(String name, Object value,
      ExtensionType defaultExtType) throws IllegalArgumentException {
    Objects.requireNonNull(emptyToNull(name));
    Collection<IndexExtensionField> ret = new ArrayList<>();
    if (value instanceof IndexExtensionField) {
      ret.add((IndexExtensionField) value);
    } else if (value instanceof Collection) {
      ((Collection<?>) value).stream().filter(Objects::nonNull)
          .forEach(val -> ret.addAll(createFields(name, val, ExtensionType.ADD)));
    } else if (value instanceof Number) {
      ret.add(createField(name, (Number) value, defaultExtType));
    } else if (value instanceof Date) {
      ret.add(createField(name, IndexFields.dateToString((Date) value), Index.NOT_ANALYZED,
          defaultExtType));
    } else if (value instanceof Temporal) {
      ret.add(createField(name, formatter(IndexFields.DATE_FORMAT).apply((Temporal) value),
          Index.NOT_ANALYZED, defaultExtType));
    } else {
      ret.add(createField(name, Objects.toString(value, ""), null, defaultExtType));
    }
    return ret;
  }

  @Override
  public Collection<IndexExtensionField> createFields(Map<String, Object> fieldMap,
      ExtensionType defaultExtType) throws IllegalArgumentException {
    return fieldMap.entrySet().stream()
        .filter(Objects::nonNull)
        .filter(entry -> nonNull(emptyToNull(entry.getKey())))
        .filter(entry -> nonNull(entry.getValue()))
        .flatMap(entry -> createFields(entry.getKey(), entry.getValue(), defaultExtType).stream())
        .collect(Collectors.toList());
  }

  @Override
  public IndexExtensionField createRemoveField(String name) {
    return createField(name, "", Index.NOT_ANALYZED, ExtensionType.REMOVE);
  }

}
