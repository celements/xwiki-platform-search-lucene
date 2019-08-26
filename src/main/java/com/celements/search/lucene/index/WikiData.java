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
package com.celements.search.lucene.index;

import static com.google.common.base.Preconditions.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.xwiki.model.reference.WikiReference;

import com.celements.search.lucene.LuceneDocType;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.plugin.lucene.IndexFields;

/**
 * Holds all data but the content of a wiki page to be indexed. The content is retrieved
 * at indexing time, which should save us some memory especially when rebuilding an index
 * for a big wiki.
 *
 * @version $Id: 9b8dc406f6212de2110fb11415ee7dd66c0bbdb0 $
 */
public class WikiData extends IndexData {

  public WikiData(WikiReference wikiReference) {
    super(LuceneDocType.none, checkNotNull(wikiReference));
  }

  @Override
  public LuceneDocId getId() {
    return new LuceneDocId(getEntityReference());
  }

  @Override
  public Term getTerm() {
    return new Term(IndexFields.DOCUMENT_WIKI, getWikiRef().getName());
  }

  @Override
  public void addDataToLuceneDocument(Document luceneDoc) throws XWikiException {
  }

}
