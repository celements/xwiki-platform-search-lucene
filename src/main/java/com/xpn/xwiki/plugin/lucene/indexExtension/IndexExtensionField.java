package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.Objects;

import org.apache.lucene.document.Fieldable;

public class IndexExtensionField {

  private Fieldable luceneField;
  private boolean replace;

  public IndexExtensionField(Fieldable field) {
    this(field, false);
  }

  public IndexExtensionField(Fieldable field, boolean replace) {
    this.luceneField = Objects.requireNonNull(field);
    this.replace = replace;
  }

  public String getName() {
    return luceneField.name();
  }

  public Fieldable getLuceneField() {
    return luceneField;
  }

  public boolean isReplace() {
    return replace;
  }

  @Override
  public String toString() {
    return "IndexExtensionField [name=" + getName() + ", luceneField=" + getLuceneField()
        + ", replace=" + isReplace() + "]";
  }

}
