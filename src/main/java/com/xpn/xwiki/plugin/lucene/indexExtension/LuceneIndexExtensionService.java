package com.xpn.xwiki.plugin.lucene.indexExtension;

import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.Requirement;

import com.xpn.xwiki.plugin.lucene.AbstractIndexData;

@Component
public class LuceneIndexExtensionService implements ILuceneIndexExtensionServiceRole {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      LuceneIndexExtensionService.class);

  @Requirement
  private List<ILuceneIndexExtender> extenders;

  @Override
  public void extend(AbstractIndexData data, Document luceneDoc) {
    for (ILuceneIndexExtender ext : extenders) {
      if (ext.isEligibleIndexData(data)) {
        for (Fieldable field : ext.getExtensionFields(data)) {
          if (field != null) {
            luceneDoc.add(field);
            LOGGER.debug("extend: extender '{}' added for data '{}' field '{}'",
                ext.getName(), data, field);
          }
        }
      } else {
        LOGGER.debug("extend: data '{}' not eligible for extender '{}'", data,
            ext.getName());
      }
    }
  }

}
