package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.List;

import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.Requirement;

import com.xpn.xwiki.plugin.lucene.AbstractIndexData;

@Component
public class LuceneIndexExtensionService implements ILuceneIndexExtensionServiceRole {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(LuceneIndexExtensionService.class);

  @Requirement
  private List<ILuceneIndexExtender> extenders;

  @Override
  public void extend(AbstractIndexData data, Document luceneDoc) {
    for (ILuceneIndexExtender ext : extenders) {
      if (ext.isEligibleIndexData(data)) {
        for (IndexExtensionField extField : ext.getExtensionFields(data)) {
          if (extField != null) {
            if (extField.isReplace()) {
              luceneDoc.removeFields(extField.getName());
            }
            luceneDoc.add(extField.getLuceneField());
            LOGGER.debug("extend: added field '{}' by extender '{}' for data '{}' ",
                extField, ext.getName(), data);
          }
        }
      } else {
        LOGGER.debug("extend: not eligible extender '{}' for data '{}' ", ext.getName(),
            data);
      }
    }
  }

}
