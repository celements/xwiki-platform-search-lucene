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
package com.celements.search.lucene.index;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.celements.search.lucene.LuceneDocType;
import com.celements.web.plugin.cmd.ConvertToPlainTextException;
import com.celements.web.plugin.cmd.PlainTextCommand;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.objects.BaseObject;
import com.xpn.xwiki.objects.BaseProperty;
import com.xpn.xwiki.objects.PropertyInterface;
import com.xpn.xwiki.objects.classes.BaseClass;
import com.xpn.xwiki.objects.classes.DateClass;
import com.xpn.xwiki.objects.classes.ListItem;
import com.xpn.xwiki.objects.classes.PasswordClass;
import com.xpn.xwiki.objects.classes.StaticListClass;

/**
 * Holds all data but the content of a wiki page to be indexed. The content is retrieved
 * at indexing time, which should save us some memory especially when rebuilding an index
 * for a big wiki.
 *
 * @version $Id: c77f17cedef64c75388de38d1f5bff305224acbc $
 */
public class DocumentData extends AbstractDocumentData {

  /** Logging helper object. */
  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentData.class);

  /** The importance of an object classname. **/
  private static final float CLASSNAME_BOOST = 0.5f;

  /** The importance of an object property. **/
  private static final float OBJECT_PROPERTY_BOOST = 0.75f;

  private PlainTextCommand plainTextCmd = new PlainTextCommand();

  public DocumentData(XWikiDocument doc, boolean deleted) {
    super(LuceneDocType.wikipage, doc, deleted);
    setAuthor(doc.getAuthor());
    setCreator(doc.getCreator());
    setModificationDate(doc.getDate());
    setCreationDate(doc.getCreationDate());
  }

  @Override
  public String getFullText(XWikiDocument doc) {
    // XXX removing xwiki adding all properties to fulltext. What should it be good for?
    // getObjectFullText(sb, doc, context);
    // TODO should it be rendered maybe by plainPageType view?
    return StringUtils.lowerCase(convertHtmlToPlainText(doc.getContent()));
  }

  private String convertHtmlToPlainText(String htmlContent) {
    try {
      return plainTextCmd.convertHtmlToPlainText(htmlContent);
    } catch (ConvertToPlainTextException exp) {
      LOGGER.error("Failed to convert html content to plain text.", exp);
    }
    return "";
  }

  /**
   * Add to the string builder, the result of
   * {@link IndexData#getFullText(XWikiDocument,XWikiContext)}plus the full text
   * content (values of title,category,content and extract ) XWiki.ArticleClass Object, as
   * far as it could be extracted.
   */
  private void getObjectFullText(StringBuilder sb, XWikiDocument doc) {
    getObjectContentAsText(sb, doc);
  }

  /**
   * Add to the string builder the value of title,category,content and extract of
   * XWiki.ArticleClass
   */
  private void getObjectContentAsText(StringBuilder sb, XWikiDocument doc) {
    for (List<BaseObject> objects : doc.getXObjects().values()) {
      for (BaseObject obj : objects) {
        extractObjectContent(sb, obj);
      }
    }
  }

  private void getObjectContentAsText(StringBuilder contentText, BaseObject baseObject,
      String property) {
    BaseProperty baseProperty = (BaseProperty) baseObject.getField(property);
    // FIXME Can baseProperty really be null?
    if ((baseProperty != null) && (baseProperty.getValue() != null)) {
      if (!(baseObject.getXClass(getContext().getXWikiContext()).getField(
          property) instanceof PasswordClass)) {
        contentText.append(convertHtmlToPlainText(
            baseProperty.getValue().toString()).toLowerCase());
      }
    }
  }

  private void extractObjectContent(StringBuilder contentText, BaseObject baseObject) {
    if (baseObject != null) {
      String[] propertyNames = baseObject.getPropertyNames();
      for (String propertyName : propertyNames) {
        getObjectContentAsText(contentText, baseObject, propertyName);
        contentText.append(" ");
      }
    }
  }

  @Override
  protected void addAdditionalData(Document luceneDoc, XWikiDocument doc) {
    for (List<BaseObject> objects : doc.getXObjects().values()) {
      for (BaseObject obj : objects) {
        if (obj != null) {
          addFieldToDocument(IndexFields.OBJECT, getModelUtils().serializeRefLocal(
              obj.getXClassReference()).toLowerCase(), Field.Store.YES, Field.Index.NOT_ANALYZED,
              CLASSNAME_BOOST, luceneDoc);
          Object[] propertyNames = obj.getPropertyNames();
          for (Object propertyName : propertyNames) {
            indexProperty(luceneDoc, obj, (String) propertyName);
          }
        }
      }
    }
  }

  private void indexProperty(Document luceneDoc, BaseObject baseObject, String propertyName) {
    String fieldFullName = baseObject.getClassName() + "." + propertyName;
    BaseClass bClass = baseObject.getXClass(getContext().getXWikiContext());
    PropertyInterface prop = bClass.getField(propertyName);

    if (prop instanceof PasswordClass) {
      // Do not index passwords
    } else if ((prop instanceof StaticListClass) && ((StaticListClass) prop).isMultiSelect()) {
      indexStaticList(luceneDoc, baseObject, (StaticListClass) prop, propertyName);
    } else if (prop instanceof DateClass) {
      // Date properties are indexed the same as document dates: formatted
      // through IndexFields.dateToString() and
      // untokenized, to be able to sort by their values.
      luceneDoc.add(new Field(fieldFullName, getContentAsDate(baseObject, propertyName),
          Field.Store.YES, Field.Index.NOT_ANALYZED));
    } else {
      StringBuilder sb = new StringBuilder();
      getObjectContentAsText(sb, baseObject, propertyName);
      addFieldToDocument(fieldFullName, sb.toString(), Field.Store.YES, Field.Index.ANALYZED,
          OBJECT_PROPERTY_BOOST, luceneDoc);
    }
  }

  private String getContentAsDate(BaseObject baseObject, String propertyName) {
    try {
      Date date = baseObject.getDateValue(propertyName);
      if (date != null) {
        return IndexFields.dateToString(date);
      }
    } catch (Exception e) {
      LOGGER.error("error getting content from  XWiki Objects ", e);
    }
    return "";
  }

  private void indexStaticList(Document luceneDoc, BaseObject baseObject, StaticListClass prop,
      String propertyName) {
    Map<String, ListItem> possibleValues = prop.getMap(getContext().getXWikiContext());
    String fieldFullName = baseObject.getClassName() + "." + propertyName;

    for (String value : (List<String>) baseObject.getListValue(propertyName)) {
      ListItem item = possibleValues.get(value);
      if (item != null) {
        // We index the key of the list
        String fieldName = fieldFullName + ".key";
        addFieldToDocument(fieldName, item.getId(), Field.Store.YES, Field.Index.ANALYZED,
            OBJECT_PROPERTY_BOOST, luceneDoc);
        // We index the value
        fieldName = fieldFullName + ".value";
        addFieldToDocument(fieldName, item.getValue(), Field.Store.YES, Field.Index.ANALYZED,
            OBJECT_PROPERTY_BOOST, luceneDoc);

        // If the key and value are not the same, we index both
        // The key is always indexed outside the if block, so here we just index
        // the value
        if (!item.getId().equals(item.getValue())) {
          addFieldToDocument(fieldFullName, item.getValue(), Field.Store.YES, Field.Index.ANALYZED,
              OBJECT_PROPERTY_BOOST, luceneDoc);
        }
      }

      addFieldToDocument(fieldFullName, value, Field.Store.YES, Field.Index.ANALYZED,
          OBJECT_PROPERTY_BOOST, luceneDoc);
    }
  }
}
