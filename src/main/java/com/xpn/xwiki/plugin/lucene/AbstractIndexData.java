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

import org.apache.lucene.index.Term;
import org.xwiki.model.EntityType;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.EntityReferenceSerializer;

import com.celements.model.context.ModelContext;
import com.celements.model.util.ModelUtils;
import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.celements.search.lucene.index.queue.IndexQueuePriorityManager;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.xpn.xwiki.web.Utils;

/**
 * @version $Id: 7078d17d13ffdd29d41a0b5367bbaf3ce545ba36 $
 * @since 1.23
 */
public abstract class AbstractIndexData implements IndexData {

  private IndexQueuePriority priority;

  private String type;

  private boolean deleted;

  private EntityReference entityReference;

  private boolean notifyObservationEvents = true;

  public AbstractIndexData(String type, EntityReference entityReference, boolean deleted) {
    this.type = checkNotNull(Strings.emptyToNull(type));
    setEntityReference(entityReference);
    setDeleted(deleted);
    if (getIndexQueuePriorityManager().isPresent()) {
      setPriority(getIndexQueuePriorityManager().get().getPriority().orNull());
    }
  }

  @Override
  public IndexQueuePriority getPriority() {
    return priority;
  }

  public AbstractIndexData setPriority(IndexQueuePriority priority) {
    this.priority = Optional.fromNullable(priority).or(IndexQueuePriority.DEFAULT);
    return this;
  }

  @Override
  public Term getTerm() {
    return new Term(IndexFields.DOCUMENT_ID, getId());
  }

  @Override
  public String getType() {
    return this.type;
  }

  /**
   * @see #isDeleted()
   */
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  public boolean isDeleted() {
    return this.deleted;
  }

  @Override
  public EntityReference getEntityReference() {
    return this.entityReference;
  }

  public void setEntityReference(EntityReference entityReference) {
    this.entityReference = entityReference;
  }

  @Override
  public boolean notifyObservationEvents() {
    return notifyObservationEvents;
  }

  public void disableObservationEventNotification() {
    this.notifyObservationEvents = false;
  }

  protected String getEntityName(EntityType type) {
    EntityReference extract = getEntityReference().extractReference(type);

    return extract != null ? extract.getName() : null;
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
        + type + "]";
  }

  protected ModelContext getContext() {
    return Utils.getComponent(ModelContext.class);
  }

  protected ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

  protected Optional<IndexQueuePriorityManager> getIndexQueuePriorityManager() {
    IndexQueuePriorityManager ret = null;
    if (Utils.getComponentManager().hasComponent(IndexQueuePriorityManager.class)) {
      ret = Utils.getComponent(IndexQueuePriorityManager.class);
    }
    return Optional.fromNullable(ret);
  }

}
