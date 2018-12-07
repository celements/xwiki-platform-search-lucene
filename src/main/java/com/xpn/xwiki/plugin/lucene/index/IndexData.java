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
package com.xpn.xwiki.plugin.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.xwiki.model.reference.EntityReference;

import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.plugin.lucene.index.queue.IndexQueuePriority;

public interface IndexData {

  /**
   * Adds this documents data to a lucene Document instance for indexing.
   * <p>
   * <strong>Short introduction to Lucene field types </strong>
   * </p>
   * <p>
   * Which type of Lucene field is used determines what Lucene does with data and how we
   * can use it for searching and showing search results:
   * </p>
   * <ul>
   * <li>Keyword fields don't get tokenized, but are searchable and stored in the index.
   * This is perfect for fields you want to search in programmatically (like ids and
   * such), and date fields. Since all user-entered queries are tokenized, letting the
   * user search these fields makes almost no sense, except of queries for date fields,
   * where tokenization is useless.</li>
   * <li>the stored text fields are used for short texts which should be searchable by the
   * user, and stored in the index for reconstruction. Perfect for document names, titles,
   * abstracts.</li>
   * <li>the unstored field takes the biggest part of the content - the full text. It is
   * tokenized and indexed, but not stored in the index. This makes sense, since when the
   * user wants to see the full content, he clicks the link to vie the full version of a
   * document, which is then delivered by xwiki.</li>
   * </ul>
   *
   * @param luceneDoc
   *          if not null, this controls which translated version of the content will be
   *          indexed. If null, the content in the default language will be used.
   */
  public void addDataToLuceneDocument(Document luceneDoc) throws XWikiException;

  /**
   * @return string unique to this document across all languages and virtual wikis
   */
  public String getId();

  public IndexQueuePriority getPriority();

  public Term getTerm();

  public String getType();

  /**
   * @return indicate of the element should be deleted from he index
   */
  public boolean isDeleted();

  public EntityReference getEntityReference();

  public boolean notifyObservationEvents();

  public String getDocumentName();

  public String getDocumentSpace();

  public String getWiki();

}
