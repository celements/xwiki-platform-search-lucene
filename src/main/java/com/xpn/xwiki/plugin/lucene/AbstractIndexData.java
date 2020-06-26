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

import static com.google.common.base.MoreObjects.*;
import static com.google.common.base.Preconditions.*;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.xwiki.model.EntityType;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.EntityReferenceSerializer;

import com.celements.model.access.exception.DocumentNotExistsException;
import com.celements.model.context.ModelContext;
import com.celements.model.util.ModelUtils;
import com.celements.search.lucene.LuceneDocType;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.xpn.xwiki.web.Utils;

/**
 * @version $Id: 7078d17d13ffdd29d41a0b5367bbaf3ce545ba36 $
 * @since 1.23
 */
public abstract class AbstractIndexData {

  private LuceneDocType type;

  private boolean deleted;

  private EntityReference entityReference;

  private boolean notifyObservationEvents = true;
  private IndexQueuePriority priority = IndexQueuePriority.DEFAULT;

  public AbstractIndexData(LuceneDocType type, EntityReference entityReference, boolean deleted) {
    this.type = checkNotNull(type);
    setEntityReference(entityReference);
    setDeleted(deleted);
  }

  /**
   * Adds this documents data to a lucene Document instance for indexing.
   * <p>
   * <strong>Short introduction to Lucene field types </strong>
   * </p>
   * <p>
   * Which type of Lucene field is used determines what Lucene does with data and how we
   * can use it for searching and showing search results:
   * </p>
   * <ul>
   * <li>Keyword fields don't get tokenized, but are searchable and stored in the index.
   * This is perfect for fields you want to search in programmatically (like ids and
   * such), and date fields. Since all user-entered queries are tokenized, letting the
   * user search these fields makes almost no sense, except of queries for date fields,
   * where tokenization is useless.</li>
   * <li>the stored text fields are used for short texts which should be searchable by the
   * user, and stored in the index for reconstruction. Perfect for document names, titles,
   * abstracts.</li>
   * <li>the unstored field takes the biggest part of the content - the full text. It is
   * tokenized and indexed, but not stored in the index. This makes sense, since when the
   * user wants to see the full content, he clicks the link to vie the full version of a
   * document, which is then delivered by xwiki.</li>
   * </ul>
   *
   * @param luceneDoc
   *          if not null, this controls which translated version of the content will be
   *          indexed. If null, the content in the default language will be used.
   */
  public void addDataToLuceneDocument(Document luceneDoc)
      throws DocumentNotExistsException {}

  /**
   * @return string unique to this document across all languages and virtual wikis
   */
  public abstract String getId();

  public Term getTerm() {
    return new Term(IndexFields.DOCUMENT_ID, getId());
  }

  public LuceneDocType getType() {
    return this.type;
  }

  /**
   * @see #isDeleted()
   */
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  /**
   * @return indicate of the element should be deleted from he index
   */
  public boolean isDeleted() {
    return this.deleted;
  }

  public EntityReference getEntityReference() {
    return this.entityReference;
  }

  public void setEntityReference(EntityReference entityReference) {
    this.entityReference = entityReference;
  }

  public boolean notifyObservationEvents() {
    return notifyObservationEvents;
  }

  public @NotNull AbstractIndexData setDisableObservationEventNotification(boolean disable) {
    this.notifyObservationEvents = !disable;
    return this;
  }

  public @NotNull IndexQueuePriority getPriority() {
    return priority;
  }

  public @NotNull AbstractIndexData setPriority(@Nullable IndexQueuePriority priority) {
    this.priority = firstNonNull(priority, IndexQueuePriority.DEFAULT);
    return this;
  }

  protected String getEntityName(EntityType type) {
    EntityReference extract = getEntityReference().extractReference(type);

    return extract != null ? extract.getName() : null;
  }

  public String getDocumentName() {
    return getEntityName(EntityType.DOCUMENT);
  }

  public String getDocumentSpace() {
    return getEntityName(EntityType.SPACE);
  }

  public String getWiki() {
    return getEntityName(EntityType.WIKI);
  }

  public String getDocumentFullName() {
    return (String) Utils.getComponent(EntityReferenceSerializer.class, "local").serialize(
        getEntityReference());
  }

  public String getFullName() {
    return (String) Utils.getComponent(EntityReferenceSerializer.class).serialize(
        getEntityReference());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " [id=" + getId() + ", deleted=" + deleted + ", type="
        + type + ", priority=" + priority + ", notifyObservationEvents=" + notifyObservationEvents
        + "]";
  }

  protected ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

  protected ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

}
