package com.celements.search.lucene.index.analysis;

import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;

public interface CelAnalyzer {

  @NotEmpty
  String filterToken(@Nullable String token);

}
