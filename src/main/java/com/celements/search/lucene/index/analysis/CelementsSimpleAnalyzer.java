package com.celements.search.lucene.index.analysis;

import static com.google.common.base.Strings.*;

import java.io.Reader;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.ASCIIFoldingFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

import com.google.common.collect.ImmutableSet;

/**
 * Filters {@link StandardTokenizer} with {@link StandardFilter}, {@link
 * LowerCaseFilter}, {@link StopFilter} and {@link ASCIIFoldingFilter}, using a list of
 * English+German stop words.
 */
public class CelementsSimpleAnalyzer extends StopwordAnalyzerBase implements CelAnalyzer {

  public static final ImmutableSet<?> STOP_WORDS = ImmutableSet.builder()
      .addAll(StandardAnalyzer.STOP_WORDS_SET.stream().map(c -> new String((char[]) c)).iterator())
      .addAll(GermanAnalyzer.getDefaultStopSet())
      .add("dass") // swiss german daß
      .build();

  public CelementsSimpleAnalyzer(Version matchVersion) {
    this(matchVersion, STOP_WORDS);
  }

  public CelementsSimpleAnalyzer(Version matchVersion, Set<?> stopWords) {
    super(matchVersion, stopWords);
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    final StandardTokenizer src = new StandardTokenizer(matchVersion, reader);
    TokenStream tok = new StandardFilter(matchVersion, src);
    tok = new LowerCaseFilter(matchVersion, tok);
    tok = new StopFilter(matchVersion, tok, stopwords);
    tok = new ASCIIFoldingFilter(tok);
    return new TokenStreamComponents(src, tok);
  }

  @Override
  public String filterToken(String token) {
    return StringUtils.stripAccents(nullToEmpty(token).toLowerCase());
  }

}
