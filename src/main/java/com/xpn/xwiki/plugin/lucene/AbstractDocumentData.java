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

import static com.google.common.base.Preconditions.*;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.model.EntityType;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.rendering.syntax.Syntax;

import com.celements.model.access.IModelAccessFacade;
import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.search.lucene.LuceneDocType;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.web.Utils;

/**
 * @version $Id: 97f3293fd1c3899d5edca377a2eb32905295a78a $
 * @since 1.23
 */
public abstract class AbstractDocumentData extends AbstractIndexData {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDocumentData.class);

  /** The importance of the document ID. **/
  protected static final float ID_BOOST = 0.1f;

  /** The importance of the document language. **/
  protected static final float LANGUAGE_BOOST = 0.1f;

  /** The importance of the entity type. **/
  protected static final float TYPE_BOOST = 0.1f;

  /** The importance of the document's wiki. **/
  protected static final float WIKI_BOOST = 0.1f;

  /** The importance of the document's space. **/
  protected static final float SPACE_BOOST = 0.5f;

  /** The importance of the document's name. **/
  protected static final float NAME_BOOST = 2.5f;

  /** The importance of the document full name. **/
  protected static final float FULL_NAME_BOOST = 2f;

  /** The importance of the document title. **/
  protected static final float TITLE_BOOST = 3f;

  /** The importance of the full document content. **/
  protected static final float CONTENT_BOOST = 2f;

  /** The importance of the document creator username. **/
  protected static final float CREATOR_BOOST = 0.25f;

  /** The importance of the last document author username. **/
  protected static final float AUTHOR_BOOST = 0.25f;

  /** The importance of the document creation date. **/
  protected static final float CREATION_DATE_BOOST = 0.2f;

  /** The importance of the document last modification date. **/
  protected static final float DATE_BOOST = 0.1f;

  /** The importance of the document hidden flag. **/
  protected static final float HIDDEN_BOOST = 0.01f;

  private String version;

  private String documentTitle;

  private String author;

  private String creator;

  private String language;

  private Date creationDate;

  private Date modificationDate;

  public AbstractDocumentData(LuceneDocType type, XWikiDocument doc, boolean deleted) {
    super(type, checkNotNull(doc).getDocumentReference(), deleted);

    setVersion(doc.getVersion());
    setDocumentTitle(doc.getRenderedTitle(Syntax.PLAIN_1_0, getContext().getXWikiContext()));
    setLanguage(doc.getLanguage());
  }

  @Override
  public void addDataToLuceneDocument(Document luceneDoc) throws DocumentNotExistsException {
    XWikiDocument doc = getModelAccess().getDocument(getDocumentReference(), getLanguage());
    addDocumentData(luceneDoc, doc);
    addAdditionalData(luceneDoc, doc);
  }

  protected abstract void addAdditionalData(Document luceneDoc, XWikiDocument doc);

  private void addDocumentData(Document luceneDoc, XWikiDocument doc) {
    LOGGER.trace("addDocumentDataToLuceneDocument: id [{}], lang [{}], wiki [{}], author [{}], "
        + "creator [{}], type [{}], date [{}], creationDate [{}], title [{}], name [{}], "
        + "space [{}], fullname [{}], hidden [{}].", getId(), getLanguage(), getWiki(), author,
        creator, getType(), modificationDate, creationDate, documentTitle, getDocumentName(),
        getDocumentSpace(), getFullName(), doc.isHidden());

    // Keyword fields: stored and indexed, but not tokenized
    addFieldToDocument(IndexFields.DOCUMENT_ID, getId(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, ID_BOOST, luceneDoc);

    addFieldToDocument(IndexFields.DOCUMENT_LANGUAGE, getLanguage(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, LANGUAGE_BOOST, luceneDoc);

    addFieldToDocument(IndexFields.DOCUMENT_WIKI, getWiki(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, WIKI_BOOST, luceneDoc);

    if (StringUtils.isNotBlank(this.author)) {
      addFieldToDocument(IndexFields.DOCUMENT_AUTHOR, this.author, Field.Store.YES,
          Field.Index.NOT_ANALYZED, AUTHOR_BOOST, luceneDoc);
    }

    if (StringUtils.isNotBlank(this.creator)) {
      addFieldToDocument(IndexFields.DOCUMENT_CREATOR, this.creator, Field.Store.YES,
          Field.Index.NOT_ANALYZED, CREATOR_BOOST, luceneDoc);
    }

    if (getType() != null) {
      addFieldToDocument(IndexFields.DOCUMENT_TYPE, getType().name(), Field.Store.YES,
          Field.Index.NOT_ANALYZED, TYPE_BOOST, luceneDoc);
    }
    if (this.modificationDate != null) {
      addFieldToDocument(IndexFields.DOCUMENT_DATE, IndexFields.dateToString(this.modificationDate),
          Field.Store.YES, Field.Index.NOT_ANALYZED, DATE_BOOST, luceneDoc);
    }
    if (this.creationDate != null) {
      addFieldToDocument(IndexFields.DOCUMENT_CREATIONDATE, IndexFields.dateToString(
          this.creationDate), Field.Store.YES, Field.Index.NOT_ANALYZED, CREATION_DATE_BOOST,
          luceneDoc);
    }

    // Short text fields: tokenized and indexed, stored in the index
    if (StringUtils.isNotBlank(this.documentTitle)) {
      addFieldToDocument(IndexFields.DOCUMENT_TITLE, this.documentTitle, Field.Store.YES,
          Field.Index.ANALYZED, TITLE_BOOST, luceneDoc);
      addFieldToDocument(IndexFields.DOCUMENT_TITLE_SORT, this.documentTitle, Field.Store.YES,
          Field.Index.NOT_ANALYZED, 0.1f, luceneDoc);
    }
    addFieldToDocument(IndexFields.DOCUMENT_NAME, getDocumentName(), Field.Store.YES,
        Field.Index.ANALYZED, NAME_BOOST, luceneDoc);
    addFieldToDocument(IndexFields.DOCUMENT_NAME_S, getDocumentName(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, NAME_BOOST, luceneDoc);

    addFieldToDocument(IndexFields.DOCUMENT_SPACE, getDocumentSpace(), Field.Store.YES,
        Field.Index.ANALYZED, SPACE_BOOST, luceneDoc);
    addFieldToDocument(IndexFields.DOCUMENT_SPACE_S, getDocumentSpace(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, SPACE_BOOST, luceneDoc);

    // Old alias for the Space, reduce the importance so that a space hit
    // doesn't score double
    addFieldToDocument(IndexFields.DOCUMENT_WEB, getDocumentSpace(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, 0.1f, luceneDoc);

    addFieldToDocument(IndexFields.DOCUMENT_FULLNAME, getDocumentFullName(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, FULL_NAME_BOOST, luceneDoc);

    addFieldToDocument(IndexFields.DOCUMENT_HIDDEN, doc.isHidden().toString(), Field.Store.YES,
        Field.Index.NOT_ANALYZED, HIDDEN_BOOST, luceneDoc);

    // Large text fields: tokenized and indexed, but not stored
    // No reconstruction of the original content will be possible from the
    // search result
    try {
      final String ft = getFullText(doc);
      if (ft != null) {
        addFieldToDocument(IndexFields.FULLTEXT, ft, Field.Store.NO, Field.Index.ANALYZED,
            CONTENT_BOOST, luceneDoc);
      }
    } catch (Exception e) {
      LOGGER.error("Error extracting fulltext for document [{}]", this.toString(), e);
    }
  }

  /**
   * @return String of documentName, documentWeb, author and creator
   */
  public abstract String getFullText(XWikiDocument doc);

  @Override
  public String getId() {
    StringBuilder retval = new StringBuilder();

    retval.append(getFullName());
    retval.append(".");
    retval.append(getLanguage());

    return retval.toString();
  }

  @Override
  public Term getTerm() {
    return new Term(IndexFields.DOCUMENT_ID, getId());
  }

  /**
   * @param author
   *          The author to set.
   */
  public void setAuthor(String author) {
    this.author = author;
  }

  /**
   * @param version
   *          the version of the document
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * @param documentTitle
   *          the document title
   */
  public void setDocumentTitle(String documentTitle) {
    this.documentTitle = documentTitle;
  }

  /**
   * @param modificationDate
   *          The modificationDate to set.
   */
  public void setModificationDate(Date modificationDate) {
    this.modificationDate = modificationDate;
  }

  public String getDocumentTitle() {
    return this.documentTitle;
  }

  public DocumentReference getDocumentReference() {
    return (DocumentReference) getEntityReference();
  }

  @Override
  public String getDocumentName() {
    return getEntityName(EntityType.DOCUMENT);
  }

  @Override
  public String getDocumentSpace() {
    return getEntityName(EntityType.SPACE);
  }

  @Override
  public String getWiki() {
    return getEntityName(EntityType.WIKI);
  }

  @Override
  public String getDocumentFullName() {
    return getModelUtils().serializeRefLocal(getEntityReference());
  }

  public String getVersion() {
    return this.version;
  }

  public Date getCreationDate() {
    return this.creationDate;
  }

  public void setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
  }

  public String getCreator() {
    return this.creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  @Override
  public String getFullName() {
    return getModelUtils().serializeRef(getEntityReference());
  }

  public String getLanguage() {
    return this.language;
  }

  public void setLanguage(String lang) {
    if (!StringUtils.isEmpty(lang)) {
      this.language = lang;
    } else {
      this.language = "default";
    }
  }

  /**
   * Indexes data into a Lucene field and adds it to the specified Lucene document.
   *
   * @param fieldName
   *          the target field name under which to index this data
   * @param value
   *          the data to index
   * @param howToStore
   *          whether or not to store this field
   * @param howToIndex
   *          how to index the data: analyzed or not
   * @param boost
   *          how much to weight hits on this field in search results
   * @param luceneDoc
   *          the Lucene document to which the resulting field should be added
   */
  protected static void addFieldToDocument(String fieldName, String value, Field.Store howToStore,
      Field.Index howToIndex, float boost, Document luceneDoc) {
    Field f = new Field(fieldName, value, howToStore, howToIndex);
    f.setBoost(boost);
    luceneDoc.add(f);
  }

  private IModelAccessFacade getModelAccess() {
    return Utils.getComponent(IModelAccessFacade.class);
  }

}
