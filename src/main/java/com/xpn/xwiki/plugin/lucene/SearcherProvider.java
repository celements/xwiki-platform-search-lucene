package com.xpn.xwiki.plugin.lucene;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.search.Searcher;

public class SearcherProvider {

  /**
   * List of Lucene indexes used for searching. By default there is only one
   * such index for all the wiki. One searches is created for each entry in
   * {@link #indexDirs}.
   */
  private Searcher[] searchers;

  private boolean markToClose;

  private Set<Thread> connectedThreads;

  public SearcherProvider(Searcher[] searchers) {
    this.searchers = searchers;
    this.markToClose = false;
    this.connectedThreads = new HashSet<Thread>();
  }

  Set<Thread> internal_getConnectedThreads() {
    return connectedThreads;
  }

  public void connect() {
    if (this.markToClose) {
      throw new IllegalStateException("you may not connect to a SearchProvider"
          + " marked to close.");
    }
    connectedThreads.add(Thread.currentThread());
  }

  public Searcher[] getSearchers() {
    if (!checkConnected()) {
      throw new IllegalStateException("you must connect to the searcher provider before"
          + " you can get any searchrs");
    }
    return this.searchers;
  }

  private boolean checkConnected() {
    return this.connectedThreads.contains(Thread.currentThread());
  }

  public void disconnect() throws IOException {
    if (connectedThreads.remove(Thread.currentThread())) {
      closeIfIdle();
    }
  }

  public boolean isMarkedToClose() {
    return this.markToClose;
  }

  public void markToClose() throws IOException {
    if (!this.markToClose) {
      this.markToClose = true;
      closeIfIdle();
    }
  }

  private void closeIfIdle() throws IOException {
    if (this.markToClose && connectedThreads.isEmpty()) {
      closeSearchers(this.searchers);
    }
  }

  /**
   * @throws IOException
   */
  void closeSearchers(Searcher[] searchers) throws IOException {
    if (searchers != null) {
      for (int i = 0; i < searchers.length; i++) {
        if (searchers[i] != null) {
          searchers[i].close();
        }
      }
    }
  }

}
