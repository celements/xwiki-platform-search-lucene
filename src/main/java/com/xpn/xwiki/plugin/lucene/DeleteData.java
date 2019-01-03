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

import static com.google.common.base.Preconditions.*;

import org.apache.lucene.document.Document;

import com.celements.search.lucene.LuceneDocType;
import com.celements.search.lucene.index.LuceneDocId;
import com.xpn.xwiki.XWikiException;

public class DeleteData extends AbstractIndexData {

  private final LuceneDocId docId;

  public DeleteData(LuceneDocId docId) {
    super(LuceneDocType.none, null, true);
    this.docId = checkNotNull(docId);
  }

  @Override
  public LuceneDocId getId() {
    return docId;
  }

  @Override
  public void addDataToLuceneDocument(Document luceneDoc) throws XWikiException {
  }

}
