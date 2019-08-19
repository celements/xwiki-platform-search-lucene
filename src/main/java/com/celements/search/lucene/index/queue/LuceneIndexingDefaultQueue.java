package com.celements.search.lucene.index.queue;

import static com.celements.configuration.ConfigSourceUtils.*;

import java.util.NoSuchElementException;

import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.Requirement;
import org.xwiki.component.phase.Initializable;
import org.xwiki.component.phase.InitializationException;
import org.xwiki.configuration.ConfigurationSource;

import com.celements.search.lucene.index.IndexData;
import com.celements.search.lucene.index.LuceneDocId;
import com.xpn.xwiki.plugin.lucene.XWikiDocumentQueue;
import com.xpn.xwiki.web.Utils;

@Component
public class LuceneIndexingDefaultQueue implements LuceneIndexingQueue, Initializable {

  static final String QUEUE_IMPL_KEY = "celements.lucene.index.queue";

  @Requirement
  private ConfigurationSource cfgSrc;

  private LuceneIndexingQueue delegate;

  @Override
  public void initialize() throws InitializationException {
    String hint = getStringProperty(cfgSrc, QUEUE_IMPL_KEY).or(XWikiDocumentQueue.NAME);
    delegate = Utils.getComponent(LuceneIndexingQueue.class, hint);
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(LuceneDocId id) {
    return delegate.contains(id);
  }

  @Override
  public void add(IndexData data) {
    delegate.add(data);
  }

  @Override
  public void put(IndexData data) throws InterruptedException, UnsupportedOperationException {
    delegate.put(data);
  }

  @Override
  public IndexData take() throws InterruptedException, UnsupportedOperationException {
    return delegate.take();
  }

  @Override
  public IndexData remove() throws NoSuchElementException {
    return delegate.remove();
  }

}
