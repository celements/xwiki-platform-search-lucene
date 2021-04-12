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

import static com.celements.common.test.CelementsTestUtils.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletContext;

import org.junit.Before;
import org.junit.Test;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.rendering.syntax.Syntax;

import com.celements.common.test.AbstractComponentTest;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.render.XWikiRenderingEngine;
import com.xpn.xwiki.web.XWikiServletContext;

/**
 * Unit tests for {@link AttachmentData}.
 *
 * @version $Id: 6235ccd3b45c79ed04c8c9ef57f99a45e11fbc01 $
 */
public class AttachmentDataTest extends AbstractComponentTest {

  private XWikiDocument document;

  private XWikiAttachment attachment;

  private AttachmentData attachmentData;

  private ServletContext servletContext;

  @Before
  public void setUp_AttachmentDataTest() throws Exception {
    DocumentReference docRef = new DocumentReference("wiki", "space", "page");
    this.document = new XWikiDocument(docRef);
    this.document.setSyntax(Syntax.XWIKI_1_0);
    this.document.setTitle(docRef.getName());
    this.attachment = new XWikiAttachment(this.document, "filename");
    this.document.getAttachmentList().add(this.attachment);

    XWikiRenderingEngine renderEngineMock = createMockAndAddToDefault(XWikiRenderingEngine.class);
    expect(getWikiMock().getRenderingEngine()).andReturn(renderEngineMock).once();
    expect(renderEngineMock.interpretText(eq(docRef.getName()), same(document), same(
        getContext()))).andReturn(docRef.getName()).once();

    servletContext = createMockAndAddToDefault(ServletContext.class);
    getContext().setEngineContext(new XWikiServletContext(servletContext));
    expect(servletContext.getAttribute(eq("javax.servlet.context.tempdir"))).andReturn(new File(
        "./", "")).anyTimes();
  }

  @Test
  public void getFullTextFromTxt() throws IOException, XWikiException {
    String filename = "txt.txt";
    String mimetype = "text/plain";
    String content = "text content\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  @Test
  public void getFullTextFromMSOffice97() throws IOException, XWikiException {
    String filename = "msoffice97.doc";
    String mimetype = "application/msword";
    String content = "ms office 97 content\n\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  @Test
  public void getFullTextFromOpenXML() throws IOException, XWikiException {
    String filename = "openxml.docx";
    String mimetype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    String content = "openxml content\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  @Test
  public void getFullTextFromOpenDocument() throws IOException, XWikiException {
    String filename = "opendocument.odt";
    String mimetype = "application/vnd.oasis.opendocument.text";
    String content = "opendocument content\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  @Test
  public void getFullTextFromPDF() throws IOException, XWikiException {
    String filename = "pdf.pdf";
    String mimetype = "application/pdf";
    String content = "\npdf content\n\n\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  @Test
  public void getFullTextFromZIP() throws IOException, XWikiException {
    String filename = "zip.zip";
    String mimetype = "application/zip";
    String content = "\nzip.txt\nzip content\n\n\n\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  @Test
  public void getFullTextFromHTML() throws IOException, XWikiException {
    String filename = "html.html";
    String mimetype = "text/html";
    String content = "something\n";
    assertFullTextAndMimeType(filename, mimetype, content);
  }

  private void assertFullTextAndMimeType(String filename, String mimetype, String content)
      throws IOException {
    attachment.setFilename(filename);
    attachment.setContent(getClass().getResourceAsStream("/" + filename));
    expect(servletContext.getMimeType(eq(filename))).andReturn(mimetype).once();
    replayDefault();
    this.attachmentData = new AttachmentData(this.attachment, false);
    verifyDefault();
    assertEquals("Wrong attachment content indexed", content, attachmentData.getFullText(document));
    assertEquals("Wrong mimetype content indexed", mimetype, attachmentData.getMimeType());
  }

}
