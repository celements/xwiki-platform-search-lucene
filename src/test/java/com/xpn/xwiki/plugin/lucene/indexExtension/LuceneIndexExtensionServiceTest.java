package com.xpn.xwiki.plugin.lucene.indexExtension;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.celements.common.test.AbstractComponentTest;
import com.xpn.xwiki.plugin.lucene.indexExtension.IndexExtensionField.ExtensionType;
import com.xpn.xwiki.web.Utils;

public class LuceneIndexExtensionServiceTest extends AbstractComponentTest {

  private LuceneIndexExtensionService indexExtService;

  @Before
  public void prepare() throws Exception {
    indexExtService = (LuceneIndexExtensionService) Utils.getComponent(
        ILuceneIndexExtensionServiceRole.class);
  }

  @Test
  public void testSingletonComponent() {
    assertSame(indexExtService, Utils.getComponent(ILuceneIndexExtensionServiceRole.class));
  }

  @Test
  public void test_createField_Number() {
    int value = 1234;
    replayDefault();
    IndexExtensionField ret = indexExtService.createField("name", value, ExtensionType.ADD);
    verifyDefault();
    assertEquals("0000001234", ret.getLuceneField().stringValue());
  }

  @Test
  public void test_createField_Number_negative() {
    int value = -1234;
    replayDefault();
    try {
      indexExtService.createField("name", value, ExtensionType.ADD);
      fail("expecting IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    verifyDefault();
  }

  @Test
  public void test_createField_Number_Long() {
    long value = 1234;
    replayDefault();
    try {
      indexExtService.createField("name", value, ExtensionType.ADD);
      fail("expecting IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    verifyDefault();
  }

}
