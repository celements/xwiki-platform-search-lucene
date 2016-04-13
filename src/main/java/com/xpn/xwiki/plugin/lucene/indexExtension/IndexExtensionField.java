package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.Objects;

import org.apache.lucene.document.Fieldable;

public class IndexExtensionField {
  
  public enum ExtensionType {
    ADD, REPLACE, REMOVE;
  }

  private ExtensionType extensionType;
  private Fieldable luceneField;

  public IndexExtensionField(Fieldable field) {
    this(ExtensionType.ADD, field);
  }

  public IndexExtensionField(Fieldable field, boolean replace) {
    this(replace ? ExtensionType.REPLACE : ExtensionType.ADD, field);
  }

  public IndexExtensionField(ExtensionType extensionType, Fieldable field) {
    this.extensionType = Objects.requireNonNull(extensionType);
    this.luceneField = Objects.requireNonNull(field);
  }

  public String getName() {
    return luceneField.name();
  }

  public ExtensionType getExtensionType() {
    return extensionType;
  }

  public Fieldable getLuceneField() {
    return luceneField;
  }
  
  public IndexExtensionField setBoost(float boost) {
    getLuceneField().setBoost(boost);
    return this;
  }

  @Override
  public String toString() {
    return "IndexExtensionField [name=" + getName() + ", extensionType="
        + getExtensionType() + ", luceneField=" + getLuceneField() + "]";
  }

}
