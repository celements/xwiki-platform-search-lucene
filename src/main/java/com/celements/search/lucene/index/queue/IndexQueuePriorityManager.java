package com.celements.search.lucene.index.queue;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.xwiki.component.annotation.ComponentRole;

import com.google.common.base.Optional;

@ComponentRole
public interface IndexQueuePriorityManager {

  @NotNull
  Optional<IndexQueuePriority> getPriority();

  void putPriority(@Nullable IndexQueuePriority priority);

}
