package com.celements.search.lucene.index;

import static com.celements.model.util.References.*;
import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.Objects;

import org.xwiki.model.EntityType;
import org.xwiki.model.reference.AttachmentReference;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;

import com.celements.model.util.ModelUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.xpn.xwiki.web.Utils;

public class LuceneDocId {

  static final String SEP = ".";
  static final String ATT_ID = "file";
  static final Splitter SPLITTER = Splitter.on(SEP).omitEmptyStrings().trimResults();
  static final Joiner JOINER = Joiner.on(SEP).skipNulls();
  static final String DEFAULT_LANG = "default";

  private final EntityReference ref;
  private final String lang;

  public LuceneDocId(EntityReference ref, String lang) {
    this.ref = checkNotNull(ref);
    this.lang = Strings.isNullOrEmpty(lang) ? DEFAULT_LANG : lang;
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
      return Objects.equals(this.getRef(), other.getRef()) && (!isLangRelevant() || Objects.equals(
          this.getLang(), other.getLang()));
    }
    return false;
  }

  private boolean isLangRelevant() {
    return extractRef(getRef(), EntityType.DOCUMENT).isPresent();
  }

  @Override
  public String toString() {
    return asString();
  }

  public String asString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getModelUtils().serializeRef(extractRef(getRef(), EntityType.DOCUMENT).or(getRef())));
    if (isLangRelevant()) {
      sb.append(".");
      sb.append(getLang());
    }
    if (extractRef(getRef(), EntityType.ATTACHMENT).isPresent()) {
      sb.append(SEP + ATT_ID + SEP);
      sb.append(getRef().getName());
    }
    return sb.toString();
  }

  public static LuceneDocId fromString(String docId) throws IllegalArgumentException {
    List<String> split = Lists.newArrayList(SPLITTER.split(docId));
    checkArgument(split.size() > 0, docId);
    EntityReference ref = getModelUtils().resolveRef(split.get(0)); // 'wiki' or 'wiki:space'
    String lang = null;
    if (split.size() > 2) {
      // 'wiki:space.doc.en'
      ref = getModelUtils().resolveRef(split.get(1), DocumentReference.class, ref);
      lang = split.get(2);
    }
    if (split.size() > 3) {
      // 'wiki:space.doc.en.file.att.jpg'
      int i = split.indexOf(ATT_ID);
      checkArgument(i == 3, docId);
      checkArgument(split.size() > 3, docId);
      String fileName = JOINER.join(split.subList(i + 1, split.size()));
      ref = getModelUtils().resolveRef(fileName, AttachmentReference.class, ref);
    }
    return new LuceneDocId(ref, lang);
  }

  private static ModelUtils getModelUtils() {
    return Utils.getComponent(ModelUtils.class);
  }

}
