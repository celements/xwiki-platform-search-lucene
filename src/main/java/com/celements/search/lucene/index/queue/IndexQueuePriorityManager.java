package com.celements.search.lucene.index.queue;

import java.util.Optional;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.xwiki.component.annotation.ComponentRole;


@ComponentRole
public interface IndexQueuePriorityManager {

  @NotNull
  Optional<IndexQueuePriority> getPriority();

  void putPriority(@Nullable IndexQueuePriority priority);

}
