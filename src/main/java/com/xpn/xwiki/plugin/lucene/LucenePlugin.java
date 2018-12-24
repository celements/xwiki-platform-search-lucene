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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.observation.ObservationManager;

import com.celements.model.util.References;
import com.celements.search.lucene.index.queue.IndexQueuePriority;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.api.Api;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;
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

  public static final String DOCTYPE_WIKIPAGE = "wikipage";

  public static final String DOCTYPE_ATTACHMENT = "attachment";

  public static final String PROP_INDEX_DIR = "xwiki.plugins.lucene.indexdir";

  public static final String PROP_ANALYZER = "xwiki.plugins.lucene.analyzer";

  public static final String PROP_RESULT_LIMIT = "xwiki.plugins.lucene.resultLimit";

  public static final String PROP_RESULT_LIMIT_WITHOUT_CHECKS = "xwiki.plugins.lucene.resultLimitWithoutChecks";

  public static final int DEFAULT_RESULT_LIMIT = 1000;

  public static final int DEFAULT_RESULT_LIMIT_WITHOUT_CHECKS = 150000;

  private static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  static final String PROP_WRITER_BUFFER_SIZE = "xwiki.plugins.lucene.writerBufferSize";

  static final String PROP_OPEN_WRITER_TRY_COUNT = "xwiki.plugins.lucene.openWriterTryCount";

  /**
   * Lucene index updater. Listens for changes and indexes wiki documents in a separate
   * thread.
   */
  volatile IndexUpdater indexUpdater;

  volatile IndexRebuilder indexRebuilder;

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
   * The current searchers provider which is still set on keepAktiv.
   */
  private SearcherProvider searcherProvider;

  /**
   * A list of directories holding Lucene index data. The first such directory is used by the
   * internal indexer. Can be configured in <tt>xwiki.cfg</tt> using the key {@link #PROP_INDEX_DIR}
   * ( <tt>xwiki.plugins.lucene.indexdir</tt>). If no directory is configured, then a subdirectory
   * <tt>lucene</tt> in the application's work directory is used.
   */
  private volatile List<Directory> indexDirs;

  private volatile boolean doRebuild = false;

  public LucenePlugin(String name, String className, XWikiContext context) {
    super(name, className, context);
    indexUpdaterExecutor = Executors.newSingleThreadExecutor();
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
   * @deprecated instead use rebuildIndex()
   */
  @Deprecated
  public int rebuildIndex(XWikiContext context) {
    return rebuildIndex() ? 0 : LucenePluginApi.REBUILD_IN_PROGRESS;
  }

  public boolean rebuildIndex() {
    return indexRebuilder.startIndexRebuild(null, Optional.<EntityReference>absent(), false);
  }

  public boolean rebuildIndex(EntityReference entityRef, boolean onlyNew) {
    Optional<WikiReference> wikiRef = References.extractRef(entityRef, WikiReference.class);
    List<WikiReference> wikis = wikiRef.isPresent() ? Arrays.asList(wikiRef.get()) : null;
    return indexRebuilder.startIndexRebuild(wikis, Optional.fromNullable(entityRef), onlyNew);
  }

  public boolean rebuildIndex(List<WikiReference> wikis, boolean onlyNew) {
    return indexRebuilder.startIndexRebuild(wikis, Optional.<EntityReference>absent(), onlyNew);
  }

  public boolean rebuildIndexWithWipe(List<WikiReference> wikis, boolean onlyNew) {
    return indexRebuilder.startIndexRebuildWithWipe(wikis, onlyNew);
  }

  /**
   * Allows to search special named lucene indexes without having to configure them in
   * <tt>xwiki.cfg</tt>. Slower than
   * {@link #getSearchResults(String, String, String, String, XWikiContext)} since new
   * index searcher instances are created for every query.
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param myIndexDirs
   *          Comma separated list of directories containing the lucene indexes to search.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @param context
   *          The context of the request.
   * @return The list of search results.
   * @throws IOException,
   *           ParseException If the index directories cannot be read, or the query is
   *           invalid.
   */
  public SearchResults getSearchResultsFromIndexes(String query, String myIndexDirs,
      String languages, XWikiContext context) throws IOException, ParseException {
    SearcherProvider mySearchers = getSearcherProviderManager().createSearchProvider(
        createSearchers(getIndexDirectories(myIndexDirs)));
    try {
      SearchResults retval = search(query, (String) null, null, languages, mySearchers, false,
          context);
      return retval;
    } finally {
      mySearchers.disconnect();
      mySearchers.markToClose();
    }
  }

  /**
   * Allows to search special named lucene indexes without having to configure them in
   * xwiki.cfg. Slower than {@link #getSearchResults}since new index searcher instances
   * are created for every query.
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortFields
   *          A list of fields to sort results by. For each field, if the name starts with
   *          '-', then that field (excluding the -) is used for reverse sorting. If
   *          <tt>null</tt> or empty, sort by hit score.
   * @param myIndexDirs
   *          Comma separated list of directories containing the lucene indexes to search.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @param context
   *          The context of the request.
   * @return The list of search results.
   * @throws IOException,
   *           ParseException If the index directories cannot be read, or the query is
   *           invalid.
   */
  public SearchResults getSearchResultsFromIndexes(String query, String[] sortFields,
      String myIndexDirs, String languages, XWikiContext context) throws IOException,
      ParseException {
    SearcherProvider mySearchers = getSearcherProviderManager().createSearchProvider(
        createSearchers(getIndexDirectories(myIndexDirs)));
    try {
      SearchResults retval = search(query, sortFields, null, languages, mySearchers, false,
          context);
      return retval;
    } finally {
      mySearchers.disconnect();
      mySearchers.markToClose();
    }
  }

  /**
   * Allows to search special named lucene indexes without having to configure them in
   * <tt>xwiki.cfg</tt>. Slower than
   * {@link #getSearchResults(String, String, String, String, XWikiContext)} since new
   * index searcher instances are created for every query.
   *
   * @param query
   *          The base query, using the query engine supported by Lucene.
   * @param sortField
   *          The name of a field to sort results by. If the name starts with '-', then
   *          the field (excluding the -) is used for reverse sorting. If <tt>null</tt> or
   *          empty, sort by hit score.
   * @param myIndexDirs
   *          Comma separated list of directories containing the lucene indexes to search.
   * @param languages
   *          Comma separated list of language codes to search in, may be <tt>null</tt> or
   *          empty to search all languages.
   * @param context
   *          The context of the request.
   * @return The list of search results.
   * @throws IOException,
   *           ParseException If the index directories cannot be read, or the query is
   *           invalid.
   */
  public SearchResults getSearchResultsFromIndexes(String query, String sortField,
      String myIndexDirs, String languages, XWikiContext context) throws IOException,
      ParseException {
    SearcherProvider mySearchers = getSearcherProviderManager().createSearchProvider(
        createSearchers(getIndexDirectories(myIndexDirs)));
    try {
      SearchResults retval = search(query, sortField, null, languages, mySearchers, false, context);
      return retval;
    } finally {
      mySearchers.disconnect();
      mySearchers.markToClose();
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
  public SearchResults getSearchResults(String query, String sortField, String virtualWikiNames,
      String languages, XWikiContext context) throws IOException, ParseException {
    return search(query, sortField, virtualWikiNames, languages, null, false, context);
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
    return search(query, sortField, virtualWikiNames, languages, null, false, context);
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
    return search(query, sortField, virtualWikiNames, languages, null, true, context);
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
      String languages, SearcherProvider theSearcherProvider, boolean skipChecks,
      XWikiContext context) throws IOException, ParseException {
    SortField sort = getSortField(sortField);

    // Perform the actual search
    return search(query, (sort != null) ? new Sort(sort) : null, virtualWikiNames, languages,
        theSearcherProvider, skipChecks, context);
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
      String languages, SearcherProvider theSearcherProvider, boolean skipChecks,
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
        theSearcherProvider, skipChecks, context);
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
      SearcherProvider theSearcherProvider, boolean skipChecks, XWikiContext context)
      throws IOException, ParseException {
    try {
      if (theSearcherProvider == null) {
        theSearcherProvider = getConnectedSearcherProvider();
      } else {
        theSearcherProvider.connect();
      }
      MultiSearcher searcher = new MultiSearcher(Iterables.toArray(
          theSearcherProvider.getSearchers(), IndexSearcher.class));
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
      LOGGER.debug("search: query [{}] returned {} hits on result with hash-id ["
          + System.identityHashCode(results) + "].", q, results.getTotalHits());

      // Transform the raw Lucene search results into XWiki-aware results
      return new SearchResults(results, searcher, theSearcherProvider, skipChecks);
    } finally {
      theSearcherProvider.disconnect();
    }
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
      this.indexRebuilder = new IndexRebuilder(indexUpdater, context);
      openSearchers();
      registerIndexUpdater();
      if (doRebuild) {
        rebuildIndex();
        LOGGER.info("Launched initial lucene indexing");
      }
      LOGGER.info("Lucene plugin initialized.");
    } catch (IOException exc) {
      LOGGER.error("Failed to open the index directory '{}'", getWriteDirectory(), exc);
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
      LOGGER.warn("getIndexDirectories - file {} exists {}", file, file.exists());
      Directory dir = FSDirectory.open(file);
      if (!IndexReader.indexExists(dir)) {
        // If there's no index create an empty one
        openWriter(dir, OpenMode.CREATE_OR_APPEND).close();
        if (indexDirs.startsWith(path)) {
          // writeIndex didn't exist, do a rebuild
          doRebuild = true;
        }
      }
      ret.add(FSDirectory.open(file));
    }
    if (ret.isEmpty()) {
      throw new IllegalArgumentException("no index directory defined");
    }
    return ret;
  }

  IndexWriter openWriter(Directory directory, OpenMode openMode) throws IOException {
    long tryCount = getContext().getWiki().ParamAsLong(PROP_OPEN_WRITER_TRY_COUNT, 10);
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
        if (tryCount-- > 0) {
          try {
            int ms = new Random().nextInt(1000);
            LOGGER.debug("failed to acquire lock on '{}', retrying in {}ms ...", directory, ms);
            Thread.sleep(ms);
          } catch (InterruptedException ex) {
            LOGGER.warn("Error while sleeping", ex);
          }
        } else {
          throw exc;
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

  /**
   * Register the Index Updater as an Event Listener so that modified documents/attachments are
   * added to the Lucene indexing queue.
   * If the Index Updater is already registered don't do anything.
   */
  private void registerIndexUpdater() {
    ObservationManager observationManager = Utils.getComponent(ObservationManager.class);
    if (observationManager.getListener(indexUpdater.getName()) == null) {
      observationManager.addListener(indexUpdater);
    }
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
   * Opens the searchers for the configured index Dirs after closing any already existing
   * ones.
   */
  protected void openSearchers() {
    getConnectedSearcherProvider(true);
  }

  private SearcherProvider getConnectedSearcherProvider() {
    return getConnectedSearcherProvider(false);
  }

  private synchronized SearcherProvider getConnectedSearcherProvider(boolean createNew) {
    if (createNew || (this.searcherProvider == null)) {
      try {
        if (this.searcherProvider != null) {
          this.searcherProvider.disconnect();
          this.searcherProvider.markToClose();
          this.searcherProvider = null;
        }
        this.searcherProvider = getSearcherProviderManager().createSearchProvider(createSearchers(
            indexDirs));
      } catch (IOException exp) {
        throw new RuntimeException("Error opening searchers for index dirs " + this.indexDirs, exp);
      }
    }
    this.searcherProvider.connect();
    return this.searcherProvider;
  }

  public Directory getWriteDirectory() {
    return Iterables.getFirst(indexDirs, null);
  }

  public long getQueueSize() {
    return this.indexUpdater.getQueueSize();
  }

  @Deprecated
  public void queueDocument(XWikiDocument doc, XWikiContext context) {
    queueDocument(doc);
  }

  public void queueDocument(XWikiDocument doc) {
    queueDocument(doc, (IndexQueuePriority) null);
  }

  public void queueDocument(XWikiDocument doc, IndexQueuePriority priorty) {
    indexUpdater.queueDocument(doc, false, priorty);
  }

  @Deprecated
  public void queueAttachment(XWikiDocument doc, XWikiAttachment attach, XWikiContext context) {
    queueAttachment(doc, attach);
  }

  public void queueAttachment(XWikiDocument doc, XWikiAttachment attach) {
    queueAttachment(doc, attach, (IndexQueuePriority) null);
  }

  public void queueAttachment(XWikiDocument doc, XWikiAttachment attach,
      IndexQueuePriority priorty) {
    indexUpdater.queueAttachment(attach, false, priorty);
  }

  @Deprecated
  public void queueAttachment(XWikiDocument doc, XWikiContext context) {
    queueAttachments(doc);
  }

  public void queueAttachments(XWikiDocument doc) {
    queueAttachments(doc, (IndexQueuePriority) null);
  }

  public void queueAttachments(XWikiDocument doc, IndexQueuePriority priorty) {
    this.indexUpdater.queueAttachments(doc, priorty);
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
    Analyzer ret;
    String analyzerClassName = getContext().getWiki().Param(PROP_ANALYZER, DEFAULT_ANALYZER);
    try {
      LOGGER.info("Instantiating analyzer '{}'", analyzerClassName);
      ret = getAnalyzerInternal(analyzerClassName);
    } catch (ReflectiveOperationException exc) {
      LOGGER.warn("Unable to instantiate analyzer '{}', using default instead ", analyzerClassName);
      try {
        ret = getAnalyzerInternal(DEFAULT_ANALYZER);
      } catch (ReflectiveOperationException exc2) {
        throw new RuntimeException("Unable to instantiate default analyzer", exc2);
      }
    }
    return ret;
  }

  /**
   * Handle a corrupt index by clearing it and rebuilding from scratch.
   */
  void handleCorruptIndex() throws IOException {
    rebuildIndex();
  }

  private XWikiContext getContext() {
    return (XWikiContext) Utils.getComponent(Execution.class).getContext().getProperty(
        XWikiContext.EXECUTIONCONTEXT_KEY);
  }

  private ISearcherProviderRole getSearcherProviderManager() {
    return Utils.getComponent(ISearcherProviderRole.class);
  }

}
