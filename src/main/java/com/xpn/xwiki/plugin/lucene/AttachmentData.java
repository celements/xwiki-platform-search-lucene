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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.celements.search.lucene.LuceneDocType;
import com.google.common.base.Strings;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;

/**
 * Holds all data but the content of an attachment to be indexed. The content is retrieved
 * at indexing time, which should save us some memory especially when rebuilding an index
 * for a big wiki.
 *
 * @version $Id: 685396ee5ad477e4d9192fc10ba72b3ba58de11f $
 */
public class AttachmentData extends AbstractDocumentData {

  /** Logging helper object. */
  private static final Logger LOGGER = LoggerFactory.getLogger(AttachmentData.class);

  /** The importance of attachments in general, compared to other documents. */
  private static final float ATTACHMENT_GLOBAL_BOOST = 0.25f;

  /** The importance of the attachment filename. */
  private static final float FILENAME_BOOST = 3f;

  /**
   * How much to weight down fields from the owner document that are not that relevant for
   * attachments.
   */
  private static final float IRRELEVANT_DOCUMENT_FIELD_BOOST = 0.1f;

  /** The importance of the attachment mimetype. */
  private static final float MIMETYPE_BOOST = 0.5f;

  /**
   * Which fields are relevant for attachments as well and should be kept at their
   * original importance.
   */
  private static final List<String> RELEVANT_DOCUMENT_FIELDS = new ArrayList<>();

  static {
    RELEVANT_DOCUMENT_FIELDS.add(IndexFields.DOCUMENT_ID);
    RELEVANT_DOCUMENT_FIELDS.add(IndexFields.DOCUMENT_TYPE);
    RELEVANT_DOCUMENT_FIELDS.add(IndexFields.DOCUMENT_AUTHOR);
    RELEVANT_DOCUMENT_FIELDS.add(IndexFields.FULLTEXT);
  }

  private int size;

  private String filename;

  private String mimetype;

  public AttachmentData(XWikiAttachment attachment, boolean deleted) {
    super(LuceneDocType.attachment, checkNotNull(attachment).getDoc(), deleted);
    setModificationDate(attachment.getDate());
    setAuthor(attachment.getAuthor());
    setSize(attachment.getFilesize());
    setFilename(attachment.getFilename());
    setMimeType(attachment.getMimeType(getContext().getXWikiContext()));
  }

  public AttachmentData(XWikiDocument document, String filename, boolean deleted) {
    super(LuceneDocType.attachment, document, deleted);
    setFilename(filename);
  }

  @Override
  protected void addAdditionalData(Document luceneDoc, XWikiDocument doc) {
    // Lower the importance of the fields inherited from the document
    List<Fieldable> existingFields = luceneDoc.getFields();
    for (Fieldable f : existingFields) {
      if (!RELEVANT_DOCUMENT_FIELDS.contains(f.name())) {
        f.setBoost(f.getBoost() * IRRELEVANT_DOCUMENT_FIELD_BOOST);
      }
    }
    if (this.filename != null) {
      addFieldToDocument(IndexFields.FILENAME, this.filename, Field.Store.YES, Field.Index.ANALYZED,
          FILENAME_BOOST, luceneDoc);
      addFieldToDocument(IndexFields.MIMETYPE, this.mimetype, Field.Store.YES, Field.Index.ANALYZED,
          MIMETYPE_BOOST, luceneDoc);
    }
    // Decrease the global score of attachments
    luceneDoc.setBoost(ATTACHMENT_GLOBAL_BOOST);
  }

  /**
   * @param size
   *          The size to set.
   */
  public void setSize(int size) {
    this.size = size;
  }

  /**
   * @return The size to set.
   */
  public int getSize() {
    return this.size;
  }

  /**
   * @return Returns the filename.
   */
  public String getFilename() {
    return this.filename;
  }

  /**
   * @param filename
   *          The filename to set.
   */
  public void setFilename(String filename) {
    this.filename = checkNotNull(Strings.emptyToNull(filename));
  }

  /**
   * @return Returns the mimetype.
   */

  public String getMimeType() {
    return this.mimetype;
  }

  /**
   * @param mimetype
   *          The mimetype to set.
   */
  public void setMimeType(String mimetype) {
    this.mimetype = mimetype;
  }

  @Override
  public String getId() {
    return new StringBuffer(super.getId()).append(".file.").append(this.filename).toString();
  }

  @Override
  public String getFullText(XWikiDocument doc) {
    return getContentAsText(doc);
  }

  private String getContentAsText(XWikiDocument doc) {
    String contentText = null;
    try {
      XWikiAttachment att = doc.getAttachment(this.filename);
      LOGGER.debug("Start parsing attachement [{}] in document [{}]", this.filename,
          doc.getDocumentReference());
      Tika tika = new Tika();
      Metadata metadata = new Metadata();
      metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, this.filename);
      try (InputStream is = getContentInputStream(att)) {
        contentText = StringUtils.lowerCase(tika.parseToString(is, metadata));
      }
    } catch (XWikiException | IOException | TikaException ex) {
      LOGGER.error("error getting content of attachment [{}] for document [{}]", this.filename,
          doc.getDocumentReference(), ex);
    } catch (LinkageError linkError) {
      // Tika parsing is prone to throwing these Errors when one of its countless dependencies are
      // messed up. This causes the whole indexer to die if we don't catch it here.
      LOGGER.error("IMPORTANT LinkageError on doc [{}] file [{}]: {}", doc.getDocumentReference(),
          filename, linkError.getMessage(), linkError);
    }
    return contentText;
  }

  /**
   * We wrap the content input stream in a BufferedInputStream to make sure that all the detectors can
   * read the content even if the input stream is configured to auto close when it reaches the end.
   * This can happen for small files if AutoCloseInputStream is used, which supports the mark and reset
   * methods so Tika uses it directly. In this case, the input stream is automatically closed after the
   * first detector reads it so the next detector fails to read it.
   * @see https://issues.apache.org/jira/browse/TIKA-2395
   * @see https://issues.apache.org/jira/browse/IO-568
   */
  private BufferedInputStream getContentInputStream(XWikiAttachment att) throws XWikiException {
    return new BufferedInputStream(att.getContentInputStream(getContext().getXWikiContext()));
  }
}
