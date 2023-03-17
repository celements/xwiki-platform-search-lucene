/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.xpn.xwiki.plugin.lucene;

import static com.celements.logging.LogUtils.*;
import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xwiki.context.Execution;

import com.celements.search.lucene.LuceneDocType;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.celements.search.lucene.index.rebuild.LuceneIndexRebuildService;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.api.Api;
import com.xpn.xwiki.api.XWiki;
import com.xpn.xwiki.plugin.XWikiDefaultPlugin;
import com.xpn.xwiki.plugin.XWikiPluginInterface;
import com.xpn.xwiki.plugin.lucene.searcherProvider.ISearcherProviderRole;
import com.xpn.xwiki.plugin.lucene.searcherProvider.SearcherProvider;
import com.xpn.xwiki.web.Utils;

/**
 * A plugin offering support for advanced searches using Lucene, a high performance, open
 * source search engine. It uses an {@link IndexUpdater} to monitor and submit wiki pages
 * for indexing to the Lucene engine, and offers simple methods for searching documents,
 * with the possiblity to sort by one or several document fields (besides the default sort
 * by relevance), filter by one or several languages, and search in one, several or all
 * virtual wikis.
 *
 * @version $Id: 9aef164ef88b71e1e854f4180a35cfdd838c2d5c $
 */
public class LucenePlugin extends XWikiDefaultPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(LucenePlugin.class);

  public static final Version VERSION = Version.LUCENE_34;

  public static final String SORT_FIELD_SCORE = "score";

  @Deprecated
  public static final String DOCTYPE_WIKIPAGE = LuceneDocType.wikipage.name();

  @Deprecated
  public static final String DOCTYPE_ATTACHMENT = LuceneDocType.attachment.name();

  public static final String PROP_INDEX_DIR = "xwiki.plugins.lucene.indexdir";

  public static final String PROP_ANALYZER = "xwiki.plugins.lucene.analyzer";

  public static final String PROP_RESULT_LIMIT = "xwiki.plugins.lucene.resultLimit";

  public static final String PROP_RESULT_LIMIT_WITHOUT_CHECKS = "xwiki.plugins.lucene.resultLimitWithoutChecks";

  public static final int DEFAULT_RESULT_LIMIT = 1000;

  public static final int DEFAULT_RESULT_LIMIT_WITHOUT_CHECKS = 150000;

  private static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  static final String PROP_WRITER_BUFFER_SIZE = "xwiki.plugins.lucene.writerBufferSize";

  /**
   * Lucene index updater. Listens for changes and indexes wiki documents in a separate
   * thread.
   */
  volatile IndexUpdater indexUpdater;

  /**
   * The Lucene text analyzer, can be configured in <tt>xwiki.cfg</tt> using the key
   * {@link #PROP_ANALYZER} ( <tt>xwiki.plugins.lucene.analyzer</tt>).
   */
  private volatile Analyzer analyzer;

  /**
   * The Executor running the index updater.
   */
  private final ExecutorService indexUpdaterExecutor;

  /**
   * The current searchers provider.
   */
  private SearcherProvider searcherProvider;

  /**
   * A list of directories holding Lucene index data. The first such directory is used by the
   * internal indexer. Can be configured in <tt>xwiki.cfg</tt> using the key {@link #PROP_INDEX_DIR}
   * ( <tt>xwiki.plugins.lucene.indexdir</tt>). If no directory is configured, then a subdirectory
   * <tt>lucene</tt> in the application's work directory is used.
   */
  private volatile List<Directory> indexDirs;

  public LucenePlugin(String name, String className, XWikiContext context) {
    super(name, className, context);
    indexUpdaterExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("IndexUpdater-%d").build());
  }

  @Override
  public String getName() {
    return "lucene";
  }

  @Override
  public Api getPluginApi(XWikiPluginInterface plugin, XWikiContext context) {
    return new LucenePluginApi((LucenePlugin) plugin, context);
  }

  @Override
  protected void finalize() throws Throwable {
    LOGGER.error("Lucene plugin will exit!");
    if (this.indexUpdater != null) {
      this.indexUpdater.doExit();
    }
    super.finalize();
  }

  /**
   * Searches all Indexes configured in <tt>xwiki.cfg</tt> (property
   * <code>xwiki.plugins.lucene.indexdir</code>).
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortField
   *          The name of a field to sort results by. If the name starts with '-', then
   *          the field (excluding the -) is used for reverse sorting. If <tt>null</tt> or
   *          empty, sort by hit score.
   * @param virtualWikiNames
   *          Comma separated list of virtual wiki names to search in, may be
   *          <tt>null</tt> to search all virtual wikis.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @return The list of search results.
   * @param context
   *          The context of the request.
   * @throws IOException,
   *           ParseException If the index directories cannot be read, or the query is
   *           invalid.
   */
  public SearchResults getSearchResults(String query, String sortField, String virtualWikiNames,
      String languages, XWikiContext context) throws IOException, ParseException {
    try (SearcherProvider searchProvider = getConnectedSearcherProvider()) {
      return search(query, sortField, virtualWikiNames, languages, searchProvider, false, context);
    }
  }

  /**
   * Searches all Indexes configured in <tt>xwiki.cfg</tt> (property
   * <code>xwiki.plugins.lucene.indexdir</code>).
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortField
   *          The name of a field to sort results by. If the name starts with '-', then
   *          the field (excluding the -) is used for reverse sorting. If <tt>null</tt> or
   *          empty, sort by hit score.
   * @param virtualWikiNames
   *          Comma separated list of virtual wiki names to search in, may be
   *          <tt>null</tt> to search all virtual wikis.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @return The list of search results.
   * @param context
   *          The context of the request.
   * @throws IOException,
   *           ParseException If the index directories cannot be read, or the query is
   *           invalid.
   */
  public SearchResults getSearchResults(String query, String[] sortField, String virtualWikiNames,
      String languages, XWikiContext context) throws IOException, ParseException {
    try (SearcherProvider searchProvider = getConnectedSearcherProvider()) {
      return search(query, sortField, virtualWikiNames, languages, searchProvider, false, context);
    }
  }

  /**
   * Searches all Indexes configured in <tt>xwiki.cfg</tt> (property
   * <code>xwiki.plugins.lucene.indexdir</code>) WITHOUT EXIST AND ACCESS CHECKS on
   * documents. Faster than {@link #getSearchResults} since database is not accessed for
   * every hit. <br>
   * <br>
   * WARNING: It is up to the calling application to assure access rights!
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortField
   *          The name of a field to sort results by. If the name starts with '-', then
   *          the field (excluding the -) is used for reverse sorting. If <tt>null</tt> or
   *          empty, sort by hit score.
   * @param virtualWikiNames
   *          Comma separated list of virtual wiki names to search in, may be
   *          <tt>null</tt> to search all virtual wikis.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @return The list of search results.
   * @param context
   *          The context of the request.
   * @throws IOException,
   *           ParseException If the index directories cannot be read, or the query is
   *           invalid.
   */
  public SearchResults getSearchResultsWithoutChecks(String query, String[] sortField,
      String virtualWikiNames, String languages, XWikiContext context) throws IOException,
      ParseException {
    try (SearcherProvider searchProvider = getConnectedSearcherProvider()) {
      return search(query, sortField, virtualWikiNames, languages, searchProvider, true, context);
    }
  }

  /**
   * Creates and submits a query to the Lucene engine.
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortField
   *          The name of a field to sort results by. If the name starts with '-', then
   *          the field (excluding the -) is used for reverse sorting. If <tt>null</tt> or
   *          empty, sort by hit score.
   * @param virtualWikiNames
   *          Comma separated list of virtual wiki names to search in, may be
   *          <tt>null</tt> to search all virtual wikis.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @param skipChecks
   *          skips exists and access checks on documents
   * @param context
   *          The context of the request.
   * @return The list of search results.
   * @throws IOException
   *           If the Lucene searchers encounter a problem reading the indexes.
   * @throws ParseException
   *           If the query is not valid.
   */
  private SearchResults search(String query, String sortField, String virtualWikiNames,
      String languages, SearcherProvider searchProvider, boolean skipChecks,
      XWikiContext context) throws IOException, ParseException {
    SortField sort = getSortField(sortField);
    return search(query, (sort != null) ? new Sort(sort) : null, virtualWikiNames, languages,
        searchProvider, skipChecks, context);
  }

  /**
   * Creates and submits a query to the Lucene engine.
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortFields
   *          A list of fields to sort results by. For each field, if the name starts with
   *          '-', then that field (excluding the -) is used for reverse sorting. If
   *          <tt>null</tt> or empty, sort by hit score.
   * @param virtualWikiNames
   *          Comma separated list of virtual wiki names to search in, may be
   *          <tt>null</tt> to search all virtual wikis.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @param skipChecks
   *          skips exists and access checks on documents
   * @param context
   *          The context of the request.
   * @return The list of search results.
   * @throws IOException
   *           If the Lucene searchers encounter a problem reading the indexes.
   * @throws ParseException
   *           If the query is not valid.
   */
  private SearchResults search(String query, String[] sortFields, String virtualWikiNames,
      String languages, SearcherProvider searchProvider, boolean skipChecks,
      XWikiContext context) throws IOException, ParseException {
    // Turn the sorting field names into SortField objects.
    SortField[] sorts = null;
    if ((sortFields != null) && (sortFields.length > 0)) {
      sorts = new SortField[sortFields.length];
      for (int i = 0; i < sortFields.length; ++i) {
        sorts[i] = getSortField(sortFields[i]);
      }
      // Remove any null values from the list.
      int prevLength = -1;
      while (prevLength != sorts.length) {
        prevLength = sorts.length;
        sorts = ArrayUtils.removeElement(sorts, null);
      }
    }

    // Perform the actual search
    return search(query, (sorts != null) ? new Sort(sorts) : null, virtualWikiNames, languages,
        searchProvider, skipChecks, context);
  }

  /**
   * Create a {@link SortField} corresponding to the field name. If the field name starts
   * with '-', then the field (excluding the leading -) will be used for reverse sorting.
   * Field name "score" will create a score {@link SortField}.
   *
   * @param sortField
   *          The name of the field to sort by. If <tt>null</tt>, return a <tt>null</tt>
   *          SortField. If starts with '-', then return a SortField that does a reverse
   *          sort on the field.
   * @return A SortFiled that sorts on the given field, or <tt>null</tt>.
   */
  private SortField getSortField(String sortField) {
    SortField sort = null;
    if (!StringUtils.isEmpty(sortField)) {
      if (sortField.equals(SORT_FIELD_SCORE)) {
        LOGGER.debug("getSortField: is field score");
        sort = SortField.FIELD_SCORE;
      } else {
        // For the moment assuming everything is a String is enough, since we
        // don't usually want to sort documents
        // on numerical object properties.
        sort = new SortField(StringUtils.removeStart(sortField, "-"), SortField.STRING,
            sortField.startsWith("-"));
      }
    }

    LOGGER.debug("getSortField: returned '" + sort + "' for field '" + sortField + "'");
    return sort;
  }

  /**
   * Creates and submits a query to the Lucene engine.
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sort
   *          A Lucene sort object, can contain one or more sort criterias. If
   *          <tt>null</tt>, sort by hit score.
   * @param virtualWikiNames
   *          Comma separated list of virtual wiki names to search in, may be
   *          <tt>null</tt> to search all virtual wikis.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @param skipChecks
   *          skips exists and access checks on documents
   * @param context
   *          The context of the request.
   * @return The list of search results.
   * @throws IOException
   *           If the Lucene searchers encounter a problem reading the indexes.
   * @throws ParseException
   *           If the query is not valid.
   */
  private SearchResults search(String query, Sort sort, String virtualWikiNames, String languages,
      SearcherProvider searchProvider, boolean skipChecks, XWikiContext context)
      throws IOException, ParseException {
    checkNotNull(searchProvider);
    MultiSearcher searcher = new MultiSearcher(Iterables.toArray(
        searchProvider.getSearchers(), IndexSearcher.class));
    // Enhance the base query with wiki names and languages.
    LOGGER.debug("build query for [{}]", query);
    Query q = buildQuery(query, virtualWikiNames, languages);
    LOGGER.debug("query is [{}]", q);
    // Perform the actual search
    int resultLimit = getResultLimit(skipChecks, context);
    TopDocsCollector<? extends ScoreDoc> results;
    if (sort != null) {
      results = TopFieldCollector.create(sort, resultLimit, true, true, false, false);
    } else {
      results = TopScoreDocCollector.create(resultLimit, false);
    }
    searcher.search(q, results);
    LOGGER.debug("search: query [{}] returned {} hits on result with hash-id [{}].",
        q, defer(() -> results.getTotalHits()), defer(() -> System.identityHashCode(results)));
    // Transform the raw Lucene search results into XWiki-aware results
    return new SearchResults(q, results, searcher, searchProvider, skipChecks, new XWiki(
        context.getWiki(), context), context);
  }

  /**
   * @param query
   * @param virtualWikiNames
   *          comma separated list of virtual wiki names
   * @param languages
   *          comma separated list of language codes to search in, may be null to search
   *          all languages
   */
  private Query buildQuery(String query, String virtualWikiNames, String languages)
      throws ParseException {
    // build a query like this: <user query string> AND <wikiNamesQuery> AND
    // <languageQuery>
    BooleanQuery bQuery = new BooleanQuery();
    Query parsedQuery = null;

    LOGGER.debug("buildQuery for [{}] with languages [{}]", query, languages);

    // for object search
    if (query.startsWith("PROP ")) {
      String property = query.substring(0, query.indexOf(":"));
      query = query.substring(query.indexOf(":") + 1, query.length());
      QueryParser qp = new QueryParser(VERSION, property, getAnalyzer());
      parsedQuery = qp.parse(query);
      bQuery.add(parsedQuery, BooleanClause.Occur.MUST);
    } else if (query.startsWith("MULTI ")) {
      // for fulltext search
      // XXX several problems in Multi search:
      // XXX 1. prefix "MULTI " not removed
      // XXX 2. IndexUpdater.fields after restart empty
      Set<String> collectedFields = indexUpdater.getCollectedFields();
      String[] fields = collectedFields.toArray(new String[collectedFields.size()]);
      BooleanClause.Occur[] flags = new BooleanClause.Occur[fields.length];
      for (int i = 0; i < flags.length; i++) {
        flags[i] = BooleanClause.Occur.SHOULD;
      }
      parsedQuery = MultiFieldQueryParser.parse(VERSION, query, fields, flags, getAnalyzer());
      bQuery.add(parsedQuery, BooleanClause.Occur.MUST);
    } else {
      String[] fields = new String[] { IndexFields.FULLTEXT, IndexFields.DOCUMENT_TITLE,
          IndexFields.DOCUMENT_NAME, IndexFields.FILENAME };
      BooleanClause.Occur[] flags = new BooleanClause.Occur[fields.length];
      for (int i = 0; i < flags.length; i++) {
        flags[i] = BooleanClause.Occur.SHOULD;
      }
      LOGGER.debug("init MultiFieldQueryParser with [{}] and analyzer {}", Arrays.toString(fields),
          getAnalyzer().getClass());
      QueryParser parser = new MultiFieldQueryParser(VERSION, fields, getAnalyzer());
      parsedQuery = parser.parse(query);
      // Since the sub-queries are OR-ed, each sub-query score is normally
      // divided by the number of sub-queries,
      // which would cause extra-small scores whenever there's a hit on only one
      // sub-query;
      // compensate this by boosting the whole outer query
      LOGGER.debug("parsed query {}", parsedQuery.toString());
      parsedQuery.setBoost(fields.length);
      bQuery.add(parsedQuery, BooleanClause.Occur.MUST);
    }

    if ((virtualWikiNames != null) && (virtualWikiNames.length() > 0)) {
      bQuery.add(buildOredTermQuery(virtualWikiNames, IndexFields.DOCUMENT_WIKI),
          BooleanClause.Occur.MUST);
    }
    if ((languages != null) && (languages.length() > 0)) {
      bQuery.add(buildOredTermQuery(languages, IndexFields.DOCUMENT_LANGUAGE),
          BooleanClause.Occur.MUST);
    }

    return bQuery;
  }

  /**
   * @param values
   *          comma separated list of values to look for
   * @return A query returning documents matching one of the given values in the given
   *         field
   */
  private Query buildOredTermQuery(final String values, final String fieldname) {
    String[] valueArray = values.split("\\,");
    if (valueArray.length > 1) {
      // build a query like this: <valueArray[0]> OR <valueArray[1]> OR ...
      BooleanQuery orQuery = new BooleanQuery();
      for (String element : valueArray) {
        orQuery.add(new TermQuery(new Term(fieldname, element.trim())), BooleanClause.Occur.SHOULD);
      }

      return orQuery;
    }

    // exactly one value, no OR'ed Terms necessary
    return new TermQuery(new Term(fieldname, valueArray[0]));
  }

  public int getResultLimit(boolean skipChecks, XWikiContext context) {
    String key = PROP_RESULT_LIMIT;
    int defaultVal = DEFAULT_RESULT_LIMIT;
    if (skipChecks) {
      key = PROP_RESULT_LIMIT_WITHOUT_CHECKS;
      defaultVal = DEFAULT_RESULT_LIMIT_WITHOUT_CHECKS;
    }
    String limitParam = context.getWiki().Param(key, Integer.toString(defaultVal));
    int limit;
    try {
      limit = Integer.parseInt(limitParam);
    } catch (NumberFormatException exc) {
      limit = defaultVal;
    }
    return limit;
  }

  @Override
  public synchronized void init(XWikiContext context) {
    LOGGER.debug("Lucene plugin: in init");
    super.init(getContext());
    try {
      indexDirs = getIndexDirectories("");
      IndexWriter writer = openWriter(getWriteDirectory(), OpenMode.CREATE_OR_APPEND);
      this.indexUpdater = new IndexUpdater(writer, this, context);
      indexUpdaterExecutor.submit(indexUpdater);
      getIndexRebuildService().initialize(indexUpdater);
      LOGGER.info("Lucene plugin initialized.");
    } catch (IOException exc) {
      LOGGER.error("Failed to open the index directory: ", exc);
      throw new RuntimeException(exc);
    }
  }

  private List<Directory> getIndexDirectories(String indexDirs) throws IOException {
    List<Directory> ret = new ArrayList<>();
    if (Strings.isNullOrEmpty(indexDirs)) {
      indexDirs = getConfiguredIndexDirs();
    }
    for (String path : indexDirs.split(",")) {
      File file = new File(path);
      if (!file.exists()) {
        file.mkdirs();
      }
      Directory dir = FSDirectory.open(file);
      if (!IndexReader.indexExists(dir)) {
        // If there's no index create an empty one
        openWriter(dir, OpenMode.CREATE_OR_APPEND).close();
      }
      ret.add(FSDirectory.open(file));
    }
    if (ret.isEmpty()) {
      throw new IllegalArgumentException("no index directory defined");
    }
    return ret;
  }

  IndexWriter openWriter(Directory directory, OpenMode openMode) throws IOException {
    IndexWriter ret = null;
    while (ret == null) {
      try {
        IndexWriterConfig cfg = new IndexWriterConfig(LucenePlugin.VERSION, getAnalyzer());
        cfg.setRAMBufferSizeMB(getContext().getWiki().ParamAsLong(PROP_WRITER_BUFFER_SIZE,
            (long) IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB));
        if (openMode != null) {
          cfg.setOpenMode(openMode);
        }
        ret = new IndexWriter(directory, cfg);
      } catch (LockObtainFailedException exc) {
        try {
          int ms = new Random().nextInt(1000);
          LOGGER.debug("failed to acquire lock, retrying in {}ms ...", ms);
          Thread.sleep(ms);
        } catch (InterruptedException ex) {
          LOGGER.warn("Error while sleeping", ex);
        }
      }
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private Analyzer getAnalyzerInternal(String analyzerClassName)
      throws ReflectiveOperationException {
    Class<? extends Analyzer> clazz = (Class<? extends Analyzer>) Class.forName(analyzerClassName);
    return clazz.getConstructor(Version.class).newInstance(VERSION);
  }

  private String getConfiguredIndexDirs() {
    String ret = getContext().getWiki().Param(PROP_INDEX_DIR);
    if (Strings.isNullOrEmpty(ret)) {
      ret = getContext().getWiki().getWorkSubdirectory("lucene", getContext()).getAbsolutePath();
    }
    return ret;
  }

  @Override
  public void flushCache(XWikiContext context) {
    LOGGER.warn("flushing cache not supported");
  }

  /**
   * Creates an array of Searchers for a number of lucene indexes.
   *
   * @return Array of searchers
   * @throws IOException
   */
  private List<IndexSearcher> createSearchers(List<Directory> indexDirs) throws IOException {
    List<IndexSearcher> ret = new ArrayList<>();
    for (Directory dir : indexDirs) {
      ret.add(new IndexSearcher(dir, true));
    }
    return ret;
  }

  /**
   * closed the current searcher (called after index commit)
   *
   * @throws IOException
   */
  protected synchronized void closeSearcherProvider() throws IOException {
    if (searcherProvider != null) {
      searcherProvider.disconnect();
      searcherProvider.markToClose();
      searcherProvider = null;
    }
  }

  /**
   * IMPORTANT: only use in try-with-resource statement
   */
  private synchronized SearcherProvider getConnectedSearcherProvider() throws IOException {
    if (searcherProvider == null) {
      searcherProvider = getSearcherProviderManager()
          .createSearchProvider(createSearchers(indexDirs));
    }
    return searcherProvider.connect();
  }

  public Directory getWriteDirectory() {
    return indexDirs.get(0);
  }

  public long getQueueSize() {
    return this.indexUpdater.getQueueSize();
  }

  public long getQueueSize(@NotNull IndexQueuePriority priority) {
    return this.indexUpdater.getQueueSize(priority);
  }

  public void queue(@NotNull AbstractIndexData data) {
    this.indexUpdater.queue(data);
  }

  /**
   * @return the number of documents Lucene index writer.
   */
  public long getLuceneDocCount() {
    return this.indexUpdater.getLuceneDocCount();
  }

  public void optimizeIndex() {
    indexUpdater.doOptimize();
  }

  public Analyzer getAnalyzer() {
    if (analyzer == null) {
      analyzer = getConfiguredAnalyzer();
    }
    return analyzer;
  }

  private Analyzer getConfiguredAnalyzer() {
    String analyzerClassName = getContext().getWiki().Param(PROP_ANALYZER, DEFAULT_ANALYZER);
    try {
      LOGGER.info("Instantiating analyzer '{}'", analyzerClassName);
      return getAnalyzerInternal(analyzerClassName);
    } catch (ReflectiveOperationException exc) {
      throw new IllegalArgumentException("Unable to instantiate analyzer", exc);
    }
  }

  private XWikiContext getContext() {
    return (XWikiContext) Utils.getComponent(Execution.class).getContext().getProperty(
        XWikiContext.EXECUTIONCONTEXT_KEY);
  }

  private ISearcherProviderRole getSearcherProviderManager() {
    return Utils.getComponent(ISearcherProviderRole.class);
  }

  private LuceneIndexRebuildService getIndexRebuildService() {
    return Utils.getComponent(LuceneIndexRebuildService.class);
  }

}
