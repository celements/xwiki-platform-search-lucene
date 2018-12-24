package com.celements.search.lucene.index;

import static com.celements.search.lucene.index.LuceneDocId.*;
import static com.google.common.base.MoreObjects.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.xwiki.model.reference.AttachmentReference;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;

import com.celements.common.test.AbstractComponentTest;

public class LuceneDocIdTest extends AbstractComponentTest {

  private WikiReference wikiRef = new WikiReference("wiki");
  private SpaceReference spaceRef = new SpaceReference("space", wikiRef);
  private DocumentReference docRef = new DocumentReference("doc", spaceRef);
  private AttachmentReference attRef = new AttachmentReference("att.jpg", docRef);

  @Test
  public void test_wiki() {
    assertDocId(wikiRef, null, "wiki");
    assertDocId(wikiRef, "en", "wiki");
  }

  @Test
  public void test_space() {
    assertDocId(spaceRef, null, "wiki:space");
    assertDocId(spaceRef, "en", "wiki:space");
  }

  @Test
  public void test_doc() {
    assertDocId(docRef, null, "wiki:space.doc.default");
    assertDocId(docRef, "en", "wiki:space.doc.en");
  }

  @Test
  public void test_att() {
    assertDocId(attRef, null, "wiki:space.doc.default.file.att.jpg");
    assertDocId(attRef, "en", "wiki:space.doc.en.file.att.jpg");
  }

  @Test
  public void test_att_ambigious() {
    AttachmentReference attRef = new AttachmentReference("file.jpg", docRef);
    assertDocId(attRef, null, "wiki:space.doc.default.file.file.jpg");
    assertDocId(attRef, "en", "wiki:space.doc.en.file.file.jpg");
  }

  private void assertDocId(EntityReference ref, String lang, String strDocId) {
    LuceneDocId docId = new LuceneDocId(ref, lang);
    assertEquals(ref, docId.getRef());
    assertEquals(firstNonNull(lang, DEFAULT_LANG), docId.getLang());
    assertEquals(strDocId, docId.asString());
    assertEquals(docId, LuceneDocId.fromString(docId.asString()));
  }

  @Test
  public void test() {
    assertEquals("wiki", new LuceneDocId(wikiRef, null).toString());
    assertEquals("wiki:space", new LuceneDocId(spaceRef, null).toString());
    assertEquals("wiki:space.doc.default", new LuceneDocId(docRef, null).toString());
    assertEquals("wiki:space.doc.default.file.att.jpg", new LuceneDocId(attRef, null).toString());
  }

  @Test
  public void test_back() {
    assertEquals(new LuceneDocId(wikiRef, null), LuceneDocId.fromString("wiki"));
    assertEquals(new LuceneDocId(spaceRef, null), LuceneDocId.fromString("wiki:space"));
    assertEquals(new LuceneDocId(docRef, null), LuceneDocId.fromString("wiki:space.doc.default"));
    assertEquals(new LuceneDocId(attRef, null), LuceneDocId.fromString(
        "wiki:space.doc.default.file.att.jpg"));
  }

}
