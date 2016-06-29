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
package com.xpn.xwiki.plugin.lucene;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.model.reference.EntityReferenceSerializer;

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
import com.xpn.xwiki.web.Utils;

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

  /** Reference serializer which removes the wiki prefix. */
  private EntityReferenceSerializer<String> localEntityReferenceSerializer = Utils.getComponent(
      EntityReferenceSerializer.class, "local");

  public DocumentData(XWikiDocument doc, boolean deleted, XWikiContext context) {
    super(LucenePlugin.DOCTYPE_WIKIPAGE, doc, deleted, context);
    setAuthor(doc.getAuthor());
    setCreator(doc.getCreator());
    setModificationDate(doc.getDate());
    setCreationDate(doc.getCreationDate());
  }

  /**
   * Append a string containing the result of {@link AbstractIndexData#getFullText} plus
   * the full text content of this document (in the given language)
   */
  @Override
  protected void getFullText(StringBuilder sb, XWikiDocument doc, XWikiContext context) {
    super.getFullText(sb, doc, context);

    sb.append(" ");
    // TODO should it be rendered maybe by plainPageType view?
    sb.append(StringUtils.lowerCase(plainTextCmd.convertToPlainText(doc.getContent())));
    sb.append(" ");

    // XXX removing xwiki adding all properties to fulltext. What should it be good for?
    // getObjectFullText(sb, doc, context);
  }

  /**
   * Add to the string builder, the result of
   * {@link AbstractIndexData#getFullText(XWikiDocument,XWikiContext)}plus the full text
   * content (values of title,category,content and extract ) XWiki.ArticleClass Object, as
   * far as it could be extracted.
   */
  private void getObjectFullText(StringBuilder sb, XWikiDocument doc, XWikiContext context) {
    getObjectContentAsText(sb, doc, context);
  }

  /**
   * Add to the string builder the value of title,category,content and extract of
   * XWiki.ArticleClass
   */
  private void getObjectContentAsText(StringBuilder sb, XWikiDocument doc, XWikiContext context) {
    for (List<BaseObject> objects : doc.getXObjects().values()) {
      for (BaseObject obj : objects) {
        extractObjectContent(sb, obj, context);
      }
    }
  }

  private void getObjectContentAsText(StringBuilder contentText, BaseObject baseObject,
      String property, XWikiContext context) {
    BaseProperty baseProperty = (BaseProperty) baseObject.getField(property);
    // FIXME Can baseProperty really be null?
    if ((baseProperty != null) && (baseProperty.getValue() != null)) {
      if (!(baseObject.getXClass(context).getField(property) instanceof PasswordClass)) {
        contentText.append(plainTextCmd.convertToPlainText(
            baseProperty.getValue().toString()).toLowerCase());
      }
    }
  }

  private void extractObjectContent(StringBuilder contentText, BaseObject baseObject,
      XWikiContext context) {
    if (baseObject != null) {
      String[] propertyNames = baseObject.getPropertyNames();
      for (String propertyName : propertyNames) {
        getObjectContentAsText(contentText, baseObject, propertyName, context);
        contentText.append(" ");
      }
    }
  }

  @Override
  public void addDocumentDataToLuceneDocument(Document luceneDoc, XWikiDocument doc,
      XWikiContext context) {
    super.addDocumentDataToLuceneDocument(luceneDoc, doc, context);

    for (List<BaseObject> objects : doc.getXObjects().values()) {
      for (BaseObject obj : objects) {
        if (obj != null) {
          addFieldToDocument(IndexFields.OBJECT, this.localEntityReferenceSerializer.serialize(
              obj.getXClassReference()).toLowerCase(), Field.Store.YES, Field.Index.NOT_ANALYZED,
              CLASSNAME_BOOST, luceneDoc);
          Object[] propertyNames = obj.getPropertyNames();
          for (Object propertyName : propertyNames) {
            indexProperty(luceneDoc, obj, (String) propertyName, context);
          }
        }
      }
    }
  }

  private void indexProperty(Document luceneDoc, BaseObject baseObject, String propertyName,
      XWikiContext context) {
    String fieldFullName = baseObject.getClassName() + "." + propertyName;
    BaseClass bClass = baseObject.getXClass(context);
    PropertyInterface prop = bClass.getField(propertyName);

    if (prop instanceof PasswordClass) {
      // Do not index passwords
    } else if ((prop instanceof StaticListClass) && ((StaticListClass) prop).isMultiSelect()) {
      indexStaticList(luceneDoc, baseObject, (StaticListClass) prop, propertyName, context);
    } else if (prop instanceof DateClass) {
      // Date properties are indexed the same as document dates: formatted
      // through IndexFields.dateToString() and
      // untokenized, to be able to sort by their values.
      luceneDoc.add(new Field(fieldFullName, getContentAsDate(baseObject, propertyName),
          Field.Store.YES, Field.Index.NOT_ANALYZED));
    } else {
      StringBuilder sb = new StringBuilder();
      getObjectContentAsText(sb, baseObject, propertyName, context);
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
      String propertyName, XWikiContext context) {
    Map<String, ListItem> possibleValues = prop.getMap(context);
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
