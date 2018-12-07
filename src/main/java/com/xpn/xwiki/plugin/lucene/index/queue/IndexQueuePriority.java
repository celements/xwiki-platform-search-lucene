package com.xpn.xwiki.plugin.lucene.index.queue;

import org.xwiki.context.Execution;
import org.xwiki.context.ExecutionContext;

import com.google.common.base.Optional;
import com.xpn.xwiki.web.Utils;

public enum IndexQueuePriority {

  LOWEST, LOW, DEFAULT, HIGH, HIGHEST;

  private static final String EXEC_CONTEXT_KEY = "lucene.index.queue.priority";

  public void putToExecutionContext() {
    getExecContext().setProperty(EXEC_CONTEXT_KEY, this);
  }

  public static Optional<IndexQueuePriority> fromExecutionContext() {
    IndexQueuePriority priority = null;
    Object property = getExecContext().getProperty(EXEC_CONTEXT_KEY);
    if (property instanceof IndexQueuePriority) {
      priority = (IndexQueuePriority) property;
    }
    return Optional.fromNullable(priority);
  }

  private static ExecutionContext getExecContext() {
    return Utils.getComponent(Execution.class).getContext();
  }

}
