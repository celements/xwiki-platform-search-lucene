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

import static org.easymock.EasyMock.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Semaphore;

import org.apache.lucene.index.IndexWriter;
import org.junit.Before;
import org.junit.Test;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.rendering.syntax.Syntax;

import com.celements.common.test.AbstractBridgedComponentTestCase;
import com.xpn.xwiki.XWiki;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.store.XWikiStoreInterface;

/**
 * Unit tests for {@link IndexUpdater}.
 *
 * @version $Id: 5f372b2802536fc296cc8ceb6eba31bdf3528d0a $
 */
public class IndexUpdaterTest extends AbstractBridgedComponentTestCase {

  private final static String INDEXDIR = "target/lucenetest";

  private final Semaphore rebuildDone = new Semaphore(0);

  private final Semaphore writeBlockerWait = new Semaphore(0);

  private final Semaphore writeBlockerAcquiresLock = new Semaphore(0);

  private XWiki mockXWiki;

  private XWikiStoreInterface mockXWikiStoreInterface;

  private XWikiDocument loremIpsum;

  private class TestIndexRebuilder extends IndexRebuilder {

    public TestIndexRebuilder(IndexUpdater indexUpdater) {
      super();
      initialize(indexUpdater);
    }

    @Override
    protected void rebuildIndexAsync(IndexRebuildFuture future) {
      IndexUpdaterTest.this.rebuildDone.release();
    }
  }

  private class TestIndexUpdater extends IndexUpdater {

    TestIndexUpdater(IndexWriter writer, LucenePlugin plugin, XWikiContext context)
        throws IOException {
      super(writer, plugin, context);
    }

    @Override
    protected void runInternal() {
      if (Thread.currentThread().getName().equals("writerBlocker")) {
        try {
          // IndexWriter writer = openWriter(OpenMode.CREATE);
          Thread.sleep(5000);
          writer.close();
        } catch (Exception e) {}
      } else if (Thread.currentThread().getName().equals("permanentBlocker")) {
        try {
          // IndexWriter writer = openWriter(OpenMode.CREATE_OR_APPEND);
          IndexUpdaterTest.this.writeBlockerAcquiresLock.release();
          IndexUpdaterTest.this.writeBlockerWait.acquireUninterruptibly();
          writer.close();
        } catch (Exception e) {}
      } else {
        super.runInternal();
      }
    }
  }

  @Before
  public void setUp_IndexUpdaterTest() throws Exception {
    loremIpsum = new TestXWikiDocument(new DocumentReference("wiki", "Lorem", "Ipsum"));
    loremIpsum.setSyntax(Syntax.XWIKI_1_0);
    loremIpsum.setAuthor("User");
    loremIpsum.setCreator("User");
    loremIpsum.setDate(new Date(0));
    loremIpsum.setCreationDate(new Date(0));
    loremIpsum.setTitle("Lorem Ipsum");
    loremIpsum.setContent("Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed"
        + " do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad"
        + " minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex"
        + " ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate"
        + " velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat"
        + " cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est"
        + " laborum.");

    mockXWikiStoreInterface = createMockAndAddToDefault(XWikiStoreInterface.class);
    mockXWikiStoreInterface.cleanUp(anyObject(XWikiContext.class));
    expectLastCall().anyTimes();

    mockXWiki = getWikiMock();
    expect(mockXWiki.getDocument(eq(this.loremIpsum.getDocumentReference()), anyObject(
        XWikiContext.class))).andReturn(loremIpsum).anyTimes();
    expect(mockXWiki.Param(eq(LucenePlugin.PROP_RESULT_LIMIT), eq("1000"))).andReturn(
        "123").anyTimes();
    expect(mockXWiki.Param(anyObject(String.class), anyObject(String.class))).andReturn(
        "").anyTimes();
    expect(mockXWiki.Param(eq(LucenePlugin.PROP_INDEX_DIR))).andReturn(
        IndexUpdaterTest.INDEXDIR).anyTimes();
    expect(mockXWiki.ParamAsLong(eq(IndexRebuilder.PROP_PAUSE_DURATION), eq(
        30L))).andReturn(1L).anyTimes();
    expect(mockXWiki.search(anyObject(String.class), anyObject(XWikiContext.class))).andReturn(
        Collections.emptyList()).anyTimes();
    expect(mockXWiki.isVirtualMode()).andReturn(false).anyTimes();
    expect(mockXWiki.getStore()).andReturn(this.mockXWikiStoreInterface).anyTimes();
    getContext().setWiki(mockXWiki);
    getContext().setDatabase("wiki");
  }

  @Test
  public void test() {
    // TODO fix and improve tests
  }

  // public void testCreateIndex() throws IOException {
  // replayDefault();
  // File f = new File(INDEXDIR);
  //
  // if (!f.exists()) {
  // f.mkdirs();
  // }
  //
  // Directory directory = FSDirectory.open(f);
  //
  // LucenePlugin plugin = new LucenePlugin("Monkey", "Monkey", getContext());
  // IndexUpdater indexUpdater = new TestIndexUpdater(directory, plugin, getContext());
  // IndexRebuilder indexRebuilder = new TestIndexRebuilder(indexUpdater, getContext());
  // indexRebuilder.startIndexRebuild();
  //
  // this.rebuildDone.acquireUninterruptibly();
  //
  // assertTrue(IndexReader.indexExists(directory));
  // verifyDefault();
  // }
  //
  // public void testIndexUpdater() throws Exception {
  // replayDefault();
  // File f = new File(INDEXDIR);
  // Directory directory;
  // if (!f.exists()) {
  // f.mkdirs();
  // }
  // directory = FSDirectory.open(f);
  //
  // LucenePlugin plugin = new LucenePlugin("Monkey", "Monkey", getContext());
  // IndexUpdater indexUpdater = new TestIndexUpdater(directory, plugin, getContext());
  // IndexRebuilder indexRebuilder = new TestIndexRebuilder(indexUpdater, getContext());
  // Thread writerBlocker = new Thread(indexUpdater, "writerBlocker");
  // writerBlocker.start();
  // plugin.init(getContext());
  // plugin.indexUpdater = indexUpdater;
  // plugin.indexRebuilder = indexRebuilder;
  //
  // // indexUpdater.wipeIndex();
  //
  // Thread indexUpdaterThread = new Thread(indexUpdater, "Lucene Index Updater");
  // indexUpdaterThread.start();
  //
  // indexUpdater.queueDocument(this.loremIpsum.clone(), false);
  // indexUpdater.queueDocument(this.loremIpsum.clone(), false);
  //
  // try {
  // Thread.sleep(1000);
  // indexUpdater.doExit();
  // } catch (InterruptedException e) {
  // }
  // while (true) {
  // try {
  // indexUpdaterThread.join();
  // break;
  // } catch (InterruptedException e) {
  // }
  // }
  //
  // Query q = new TermQuery(new Term(IndexFields.DOCUMENT_ID, "wiki:Lorem.Ipsum.default"));
  // IndexSearcher searcher = null;
  // try {
  // searcher = new IndexSearcher(directory, true);
  // TopDocs t = searcher.search(q, null, 10);
  // // assertEquals(1, t.totalHits); FIXME this sometimes fails when releasing ...
  // } finally {
  // IOUtils.closeQuietly(searcher);
  // }
  //
  // SearchResults results = plugin.getSearchResultsFromIndexes("Ipsum", "target/lucenetest", null,
  // getContext());
  //
  // assertEquals(1, results.getTotalHitcount());
  // verifyDefault();
  // }
  //
  // public void testLock() throws IOException {
  // replayDefault();
  // Directory directory;
  // File f = new File(INDEXDIR);
  //
  // if (!f.exists()) {
  // f.mkdirs();
  // }
  // directory = FSDirectory.open(f);
  //
  // LucenePlugin plugin = new LucenePlugin("Monkey", "Monkey", getContext());
  //
  // final IndexUpdater indexUpdater = new TestIndexUpdater(directory, plugin, getContext());
  //
  // plugin.init(getContext());
  // plugin.indexUpdater = indexUpdater;
  //
  // Thread permanentBlocker = new Thread(indexUpdater, "permanentBlocker");
  // permanentBlocker.start();
  // this.writeBlockerAcquiresLock.acquireUninterruptibly();
  //
  // assertTrue(IndexWriter.isLocked(indexUpdater.getDirectory()));
  //
  // final boolean[] doneCleaningIndex = { false };
  //
  // Thread indexCleaner = new Thread(new Runnable() {
  //
  // @Override
  // public void run() {
  // // indexUpdater.wipeIndex();
  //
  // doneCleaningIndex[0] = true;
  // }
  // }, "indexCleaner");
  //
  // indexCleaner.start();
  //
  // try {
  // Thread.sleep(5000);
  // } catch (InterruptedException e) {
  // }
  //
  // assertFalse(doneCleaningIndex[0]);
  //
  // boolean wasActuallyLocked = false;
  //
  // try {
  // if (!IndexWriter.isLocked(indexUpdater.getDirectory())) {
  // new IndexWriter(indexUpdater.getDirectory(), new StandardAnalyzer(Version.LUCENE_34),
  // MaxFieldLength.LIMITED);
  // } else {
  // wasActuallyLocked = true;
  // }
  // // assert(IndexWriter.isLocked(indexUpdater.getDirectory()));
  // } catch (LockObtainFailedException e) {
  // /*
  // * Strange, the isLocked method appears to be unreliable.
  // */
  // wasActuallyLocked = true;
  // }
  //
  // assertTrue(wasActuallyLocked);
  //
  // this.writeBlockerWait.release();
  //
  // while (true) {
  // try {
  // indexCleaner.join();
  // break;
  // } catch (InterruptedException e) {
  // }
  // }
  //
  // assertTrue(doneCleaningIndex[0]);
  // assertFalse(IndexWriter.isLocked(indexUpdater.getDirectory()));
  // IndexWriter w = null;
  // try {
  // new IndexWriter(indexUpdater.getDirectory(), new StandardAnalyzer(Version.LUCENE_34),
  // MaxFieldLength.LIMITED);
  // } finally {
  // IOUtils.closeQuietly(w);
  // }
  // verifyDefault();
  // }

}
