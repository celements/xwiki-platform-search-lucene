package com.xpn.xwiki.plugin.lucene.indexExtension;

import static com.celements.common.MoreOptional.*;
import static com.celements.common.date.DateFormat.*;
import static com.google.common.base.Strings.*;

import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Fieldable;
import org.jsoup.Jsoup;

import com.google.common.primitives.Doubles;
import com.xpn.xwiki.plugin.lucene.IndexFields;

import one.util.streamex.EntryStream;

public class IndexExtensionField {

  public enum ExtensionType {
    ADD, REPLACE, REMOVE;
  }

  private final ExtensionType extensionType;
  private final Fieldable luceneField;

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField.Builder}
   */
  @Deprecated
  public IndexExtensionField(Fieldable field) {
    this(ExtensionType.ADD, field);
  }

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField.Builder}
   */
  @Deprecated
  public IndexExtensionField(Fieldable field, boolean replace) {
    this(replace ? ExtensionType.REPLACE : ExtensionType.ADD, field);
  }

  /**
   * @deprecated since 5.9, instead use {@link IndexExtensionField.Builder}
   */
  @Deprecated // change to protected instead of remove
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

  @Override
  public String toString() {
    return "IndexExtensionField [name=" + getName() + ", extensionType=" + getExtensionType()
        + ", luceneField=" + getLuceneField() + "]";
  }

  public static class Builder {

    private final String name;
    private ExtensionType extensionType;
    private Field.Store store;
    private Field.Index index;
    private Float boost;
    private String value = "";

    public Builder(@NotBlank String name) {
      this.name = asNonBlank(name).orElseThrow(IllegalArgumentException::new);
    }

    public Builder extensionType(@Nullable ExtensionType extensionType) {
      this.extensionType = extensionType;
      return this;
    }

    public Builder store(@Nullable Field.Store store) {
      this.store = store;
      return this;
    }

    public Builder index(@Nullable Field.Index index) {
      this.index = index;
      return this;
    }

    public Builder boost(@Nullable Float boost) {
      this.boost = boost;
      return this;
    }

    public Builder value(@Nullable String value) {
      this.value = Jsoup.parse(nullToEmpty(value)).text().toLowerCase();
      return this;
    }

    public Builder value(@Nullable Number value) {
      return value(IndexFields.numberToString(value))
          .index(Index.NOT_ANALYZED);
    }

    public Builder value(@Nullable Boolean value) {
      return value((value != null) ? Boolean.toString(value) : "")
          .index(Index.NOT_ANALYZED);
    }

    public Builder value(@Nullable Date value) {
      return value((value != null) ? IndexFields.dateToString(value) : "")
          .index(Index.NOT_ANALYZED);
    }

    public Builder value(@Nullable Temporal value) {
      return value((value != null) ? formatter(IndexFields.DATE_FORMAT).apply(value) : "")
          .index(Index.NOT_ANALYZED);
    }

    public Builder value(@Nullable Object value) {
      if (value instanceof Number) {
        return value((Number) value);
      } else if (value instanceof Boolean) {
        return value((Boolean) value);
      } else if (value instanceof Date) {
        return value((Date) value);
      } else if (value instanceof Temporal) {
        return value((Temporal) value);
      } else {
        return value(Objects.toString(value, ""));
      }
    }

    public IndexExtensionField build() {
      ExtensionType extType = Optional.ofNullable(extensionType).orElse(ExtensionType.REPLACE);
      Fieldable field = new Field(name, value,
          Optional.ofNullable(store).orElse(Field.Store.YES),
          Optional.ofNullable(index).orElseGet(this::determineIndexByNameOrValue));
      field.setBoost(Optional.ofNullable(boost).orElse(1.0f));
      return new IndexExtensionField(extType, field);
    }

    private Field.Index determineIndexByNameOrValue() {
      String name = this.name.toLowerCase();
      return name.endsWith("_s") || name.endsWith("_fullname")
          || value.equals("true") || value.equals("false")
          || (Doubles.tryParse(value) != null)
              ? Field.Index.NOT_ANALYZED
              : Field.Index.ANALYZED;
    }
  }

  public static Stream<IndexExtensionField> createFromValue(String name, Object value,
      ExtensionType extensionType) throws IllegalArgumentException {
    if (value instanceof IndexExtensionField) {
      return Stream.of((IndexExtensionField) value);
    } else if (value instanceof Collection) {
      return ((Collection<?>) value).stream()
          .filter(Objects::nonNull)
          .flatMap(val -> createFromValue(name, val, ExtensionType.ADD));
    } else {
      return Stream.of(new IndexExtensionField.Builder(name)
          .extensionType(extensionType)
          .value(value)
          .build());
    }
  }

  public static Stream<IndexExtensionField> createFromMap(Map<String, Object> fieldMap,
      ExtensionType extensionType) throws IllegalArgumentException {
    return EntryStream.of(fieldMap)
        .filterKeys(name -> !nullToEmpty(name).trim().isEmpty())
        .filterValues(Objects::nonNull)
        .flatMap(entry -> createFromValue(entry.getKey(), entry.getValue(), extensionType));
  }

  public static IndexExtensionField createRemove(String name) {
    return new IndexExtensionField.Builder(name)
        .extensionType(ExtensionType.REMOVE)
        .index(Index.NOT_ANALYZED)
        .value("")
        .build();
  }

}
