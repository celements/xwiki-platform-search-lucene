package com.xpn.xwiki.plugin.lucene.observation.event;

import org.xwiki.model.reference.EntityReference;

import com.celements.common.observation.converter.Local;
import com.celements.common.observation.event.AbstractEntityEvent;

@Local
public class LuceneDocumentDeletedEvent extends AbstractEntityEvent {

  private static final long serialVersionUID = 1L;

  public LuceneDocumentDeletedEvent() {
    super();
  }

  public LuceneDocumentDeletedEvent(EntityReference reference) {
    super(reference);
  }

}
