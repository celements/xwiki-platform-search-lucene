package com.celements.search.lucene.index;

import static com.celements.model.util.References.*;
import static com.google.common.base.MoreObjects.*;
import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.xwiki.model.EntityType;
import org.xwiki.model.reference.AttachmentReference;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;

import com.celements.model.util.ModelUtils;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.xpn.xwiki.web.Utils;

public class LuceneDocId {

  static final String DEFAULT_LANG = "default";
  static final String ATTACHMENT_KEYWORD = "file";
  static final Splitter SPLITTER = Splitter.on('.').omitEmptyStrings().trimResults();

  private final EntityReference ref;
  private final String lang;

  public LuceneDocId(EntityReference ref) {
    this(ref, null);
  }

  public LuceneDocId(EntityReference ref, String lang) {
    this.ref = checkNotNull(ref);
    this.lang = firstNonNull(Strings.emptyToNull(lang), DEFAULT_LANG);
  }

  public EntityReference getRef() {
    return ref;
  }

  public String getLang() {
    return lang;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRef(), getLang());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LuceneDocId) {
      LuceneDocId other = (LuceneDocId) obj;
      return Objects.equals(this.getRef(), other.getRef())
          && (!isLangDependent() || Objects.equals(this.getLang(), other.getLang()));
    }
    return false;
  }

  private boolean isLangDependent() {
    return extractRef(getRef(), EntityType.DOCUMENT).isPresent();
  }

  @Override
  public String toString() {
    return serialize();
  }

  /**
   * @return 'wiki:space.doc.en.file.att.jpg'
   */
  public String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append(getModelUtils().serializeRef(extractRef(getRef(), EntityType.DOCUMENT)
        .or(getRef())));
    if (isLangDependent()) {
      sb.append('.');
      sb.append(getLang());
    }
    if (extractRef(getRef(), EntityType.ATTACHMENT).isPresent()) {
      sb.append("." + ATTACHMENT_KEYWORD + ".");
      sb.append(getRef().getName());
    }
    return sb.toString();
  }

  public static LuceneDocId parse(String docId) {
    List<String> split = SPLITTER.splitToList(docId);
    checkArgument(!split.isEmpty(), docId);
    EntityReference ref = getModelUtils().resolveRef(split.get(0)); // 'wiki' or 'wiki:space'
    if (split.size() > 1) { // 'wiki:space.doc'
      ref = getModelUtils().resolveRef(split.get(1), DocumentReference.class, ref);
    }
    String lang = DEFAULT_LANG;
    if (split.size() > 2) { // 'wiki:space.doc.en'
      lang = split.get(2);
      checkArgument(DEFAULT_LANG.equals(lang) || lang.length() == 2, docId);
    }
    if (split.size() > 3) { // 'wiki:space.doc.en.file.att.jpg'
      String fileName = extractFileName(split)
          .orElseThrow(() -> new IllegalArgumentException(docId));
      ref = getModelUtils().resolveRef(fileName, AttachmentReference.class, ref);
    }
    return new LuceneDocId(ref, lang);
  }

  private static Optional<String> extractFileName(List<String> split) {
    int i = split.indexOf(ATTACHMENT_KEYWORD);
    if ((i >= 2) && ((i + 1) < split.size())  ) {
      return Optional.of(split.subList(i + 1, split.size()).stream()
          .filter(Objects::nonNull)
          .collect(Collectors.joining(".")));
    }
    return Optional.empty();
  }

  private static ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

}
