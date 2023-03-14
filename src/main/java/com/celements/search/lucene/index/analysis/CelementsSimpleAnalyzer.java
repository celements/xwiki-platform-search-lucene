package com.celements.search.lucene.index.analysis;

import java.io.Reader;
import java.util.Set;

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
public class CelementsSimpleAnalyzer extends StopwordAnalyzerBase {

  public static final ImmutableSet<?> STOP_WORDS = ImmutableSet.builder()
      .addAll(StandardAnalyzer.STOP_WORDS_SET)
      .addAll(GermanAnalyzer.getDefaultStopSet())
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
}
