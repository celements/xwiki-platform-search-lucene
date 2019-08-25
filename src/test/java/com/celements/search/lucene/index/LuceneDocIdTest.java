package com.celements.search.lucene.index;

import static com.celements.common.test.CelementsTestUtils.*;
import static com.google.common.base.MoreObjects.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.xwiki.model.reference.AttachmentReference;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.SpaceReference;
import org.xwiki.model.reference.WikiReference;

import com.celements.common.test.AbstractComponentTest;
import com.celements.common.test.ExceptionAsserter;

public class LuceneDocIdTest extends AbstractComponentTest {

  private WikiReference wikiRef = new WikiReference("wiki");
  private SpaceReference spaceRef = new SpaceReference("space", wikiRef);
  private DocumentReference docRef = new DocumentReference("doc", spaceRef);
  private AttachmentReference attRef = new AttachmentReference("att.jpg", docRef);

  @Test
  public void prepareTest() {
    getContext().setDatabase(wikiRef.getName());
  }

  @Test
  public void test_wiki() {
    assertDocId(wikiRef, "wiki");
    assertDocId(wikiRef, "wiki");
  }

  @Test
  public void test_space() {
    assertDocId(spaceRef, "wiki:space");
    assertDocId(spaceRef, "wiki:space");
  }

  private void assertDocId(EntityReference ref, String strDocId) {
    assertDocId(ref, "", strDocId, true);
  }

  @Test
  public void test_doc() {
    assertDocId(docRef, null, "wiki:space.doc.default");
    assertDocId(docRef, "en", "wiki:space.doc.en");
    assertDocId(docRef, null, "space.doc.default", false);
  }

  @Test
  public void test_att() {
    assertDocId(attRef, null, "wiki:space.doc.default.file.att.jpg");
    assertDocId(attRef, "en", "wiki:space.doc.en.file.att.jpg");
    assertDocId(attRef, null, "space.doc.default.file.att.jpg", false);
  }

  @Test
  public void test_att_ambigious() {
    AttachmentReference attRef = new AttachmentReference("file.jpg", docRef);
    assertDocId(attRef, null, "wiki:space.doc.default.file.file.jpg");
    assertDocId(attRef, "en", "wiki:space.doc.en.file.file.jpg");
  }

  private void assertDocId(EntityReference ref, String lang, String strDocId) {
    assertDocId(ref, lang, strDocId, true);
  }

  private void assertDocId(EntityReference ref, String lang, String strDocId, boolean strict) {
    LuceneDocId docId = new LuceneDocId(ref, lang);
    assertEquals(ref, docId.getRef());
    assertEquals(firstNonNull(lang, LuceneDocId.DEFAULT_LANG), docId.getLang());
    assertEquals(docId, LuceneDocId.parse(docId.serialize()));
    if (strict) {
      assertEquals(strDocId, docId.serialize());
    }
  }

  @Test
  public void test_parse_illegal() {
    assertIllegalDocId(null);
    assertIllegalDocId("");
    assertIllegalDocId(".");
    assertIllegalDocId("space.doc");
    assertIllegalDocId("space.doc.parsel");
    assertIllegalDocId("space.doc.en.file");
    assertIllegalDocId("space.doc.en.illegal.stuff");
    assertIllegalDocId("space.doc.en.more.illegal.file.stuff");
  }

  private void assertIllegalDocId(String strDocId) {
    new ExceptionAsserter<IllegalArgumentException>(IllegalArgumentException.class) {

      @Override
      protected void execute() throws Exception {
        LuceneDocId.parse(strDocId);
      }
    }.evaluate();

  }

}
