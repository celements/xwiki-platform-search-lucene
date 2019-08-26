package com.celements.search.lucene.index;

import static com.celements.common.MoreObjectsCel.*;
import static com.celements.model.util.References.*;
import static com.google.common.base.MoreObjects.*;
import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Strings.*;

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
    if (ref instanceof DocumentReference) {
      this.lang = firstNonNull(emptyToNull(lang), DEFAULT_LANG);
    } else {
      this.lang = "";
    }
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
    return tryCast(obj, LuceneDocId.class)
        .map(other -> Objects.equals(this.getRef(), other.getRef())
            && Objects.equals(this.getLang(), other.getLang()))
        .orElse(false);
  }

  @Override
  public String toString() {
    return serialize();
  }

  /**
   * wiki: 'wiki',
   * doc: 'wiki:space.doc.en',
   * att: 'wiki:space.doc.file.att.jpg'
   */
  public String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append(getModelUtils().serializeRef(extractRef(getRef(), EntityType.DOCUMENT)
        .or(getRef())));
    if (!getLang().isEmpty()) {
      sb.append('.');
      sb.append(getLang());
    }
    if (getRef().getType() == EntityType.ATTACHMENT) {
      sb.append("." + ATTACHMENT_KEYWORD + ".");
      sb.append(getRef().getName());
    }
    return sb.toString();
  }

  public static LuceneDocId parse(String docId) {
    List<String> split = SPLITTER.splitToList(nullToEmpty(docId));
    checkArgument(!split.isEmpty(), docId);
    EntityReference ref = getModelUtils().resolveRef(split.get(0)); // 'wiki' or 'wiki:space'
    if (split.size() > 1) { // 'wiki:space.doc'
      ref = getModelUtils().resolveRef(split.get(1), DocumentReference.class, ref);
    }
    String lang = DEFAULT_LANG;
    Optional<String> attachmentFileName = extractAttachmentFileName(split);
    if (attachmentFileName.isPresent()) { // 'wiki:space.doc.file.att.jpg'
      ref = getModelUtils().resolveRef(attachmentFileName.get(), AttachmentReference.class, ref);
    } else if (split.size() > 2) { // 'wiki:space.doc.en'
      lang = split.get(2);
      checkArgument(DEFAULT_LANG.equals(lang) || (lang.length() == 2), docId);
    }
    return new LuceneDocId(ref, lang);
  }

  private static Optional<String> extractAttachmentFileName(List<String> split) {
    int i = split.indexOf(ATTACHMENT_KEYWORD);
    if ((i >= 2) && ((i + 1) < split.size())) {
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
