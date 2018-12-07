package com.xpn.xwiki.plugin.lucene.index.queue;

import org.xwiki.component.annotation.ComponentRole;

import com.xpn.xwiki.plugin.lucene.index.IndexData;

@ComponentRole
public interface LuceneIndexingQueue {

  boolean contains(String id);

  void add(IndexData data);

  IndexData remove();

  boolean isEmpty();

  int getSize();

}
