package com.xpn.xwiki.plugin.lucene.indexExtension;

import static org.junit.Assert.*;

import java.time.LocalDate;

import org.apache.lucene.document.Field;
import org.junit.Before;
import org.junit.Test;

import com.xpn.xwiki.plugin.lucene.indexExtension.IndexExtensionField.ExtensionType;

public class IndexExtensionFieldTest {

  @Before
  public void prepare() throws Exception {}

  @Test
  public void test_defaults() {
    String name = "afield";
    IndexExtensionField field = new IndexExtensionField.Builder(name).build();
    assertEquals(name, field.getName());
    assertEquals(ExtensionType.REPLACE, field.getExtensionType());
    assertEquals(name, field.getLuceneField().name());
    assertEquals("", field.getLuceneField().stringValue());
    assertTrue(field.getLuceneField().isStored());
    assertTrue(field.getLuceneField().isIndexed());
    assertTrue(field.getLuceneField().isTokenized());
    assertEquals(1f, field.getLuceneField().getBoost(), 0);
  }

  @Test
  public void test_IllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> new IndexExtensionField.Builder(null));
    assertThrows(IllegalArgumentException.class, () -> new IndexExtensionField.Builder(""));
    assertThrows(IllegalArgumentException.class, () -> new IndexExtensionField.Builder(" "));
  }

  @Test
  public void test_extensionType() {
    for (ExtensionType type : ExtensionType.values()) {
      assertEquals(type, new IndexExtensionField.Builder("afield").extensionType(type).build()
          .getExtensionType());
    }
  }

  @Test
  public void test_store() {
    for (Field.Store store : Field.Store.values()) {
      assertEquals(store.isStored(), new IndexExtensionField.Builder("afield").store(store).build()
          .getLuceneField()
          .isStored());
    }
  }

  @Test
  public void test_index() {
    for (Field.Index index : Field.Index.values()) {
      IndexExtensionField field = new IndexExtensionField.Builder("afield").index(index).build();
      assertEquals(index.isIndexed(), field.getLuceneField().isIndexed());
      assertEquals(index.isAnalyzed(), field.getLuceneField().isTokenized());
    }
  }

  @Test
  public void test_boost() {
    float boost = 1.23f;
    assertEquals(boost, new IndexExtensionField.Builder("afield").boost(boost).build()
        .getLuceneField().getBoost(), 0);
  }

  @Test
  public void test_value_string() {
    Object value = "text";
    IndexExtensionField field = new IndexExtensionField.Builder("afield").value(value).build();
    assertEquals(value, field.getLuceneField().stringValue());
    assertTrue(field.getLuceneField().isTokenized());
  }

  @Test
  public void test_value_html() {
    Object value = "text";
    IndexExtensionField field = new IndexExtensionField.Builder("afield")
        .value("<body>" + value + "</body>").build();
    assertEquals(value, field.getLuceneField().stringValue());
  }

  @Test
  public void test_value_number() {
    Object value = 5;
    IndexExtensionField field = new IndexExtensionField.Builder("afield").value(value).build();
    assertEquals("0000000005", field.getLuceneField().stringValue());
    assertFalse(field.getLuceneField().isTokenized());
  }

  @Test
  public void test_value_bool() {
    Object value = true;
    IndexExtensionField field = new IndexExtensionField.Builder("afield").value(value).build();
    assertEquals("true", field.getLuceneField().stringValue());
    assertFalse(field.getLuceneField().isTokenized());
  }

  @Test
  public void test_value_temporal() {
    Object value = LocalDate.of(2021, 02, 11).atTime(13, 15);
    IndexExtensionField field = new IndexExtensionField.Builder("afield").value(value).build();
    assertEquals("202102111315", field.getLuceneField().stringValue());
    assertFalse(field.getLuceneField().isTokenized());
  }

  @Test
  public void test_determineType() {
    assertTrue(new IndexExtensionField.Builder("afield").build()
        .getLuceneField().isTokenized());
    assertFalse(new IndexExtensionField.Builder("afield_s").build()
        .getLuceneField().isTokenized());
    assertFalse(new IndexExtensionField.Builder("afield_fullname").build()
        .getLuceneField().isTokenized());
    assertFalse(new IndexExtensionField.Builder("afield").value("true").build()
        .getLuceneField().isTokenized());
    assertFalse(new IndexExtensionField.Builder("afield").value("false").build()
        .getLuceneField().isTokenized());
    assertFalse(new IndexExtensionField.Builder("afield").value("123").build()
        .getLuceneField().isTokenized());
    assertFalse(new IndexExtensionField.Builder("afield").value("123.456").build()
        .getLuceneField().isTokenized());
  }
}
