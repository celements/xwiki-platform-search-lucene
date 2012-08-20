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

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletContext;

import org.junit.Before;
import org.junit.Test;
import org.xwiki.environment.Environment;
import org.xwiki.environment.internal.ServletEnvironment;
import org.xwiki.model.reference.DocumentReference;

import com.celements.common.test.AbstractBridgedComponentTestCase;
import com.xpn.xwiki.doc.XWikiAttachment;
import com.xpn.xwiki.doc.XWikiDocument;

/**
 * Unit tests for {@link AttachmentData}.
 * 
 * @version $Id: 6235ccd3b45c79ed04c8c9ef57f99a45e11fbc01 $
 */
public class AttachmentDataTest extends AbstractBridgedComponentTestCase {
  private XWikiDocument document;

  private XWikiAttachment attachment;

  private AttachmentData attachmentData;

  private ServletContext servletContext;

  @Before
  public void setUp_AttachmentDataTest() throws Exception {
    this.document = new XWikiDocument(new DocumentReference("wiki", "space", "page"));
    this.attachment = new XWikiAttachment(this.document, "filename");
    this.document.getAttachmentList().add(this.attachment);

    this.attachmentData = new AttachmentData(this.attachment, getContext(), false);
    ServletEnvironment env = (ServletEnvironment)getComponentManager().getInstance(
        Environment.class);
    servletContext = createMock(ServletContext.class);
    env.setServletContext(servletContext);
    expect(servletContext.getAttribute(eq("javax.servlet.context.tempdir"))).andReturn(
        new File("./", "")).anyTimes();
  }

  private void assertGetFullText(String expect, String filename) throws IOException {
    this.attachment.setFilename(filename);
    this.attachment.setContent(getClass().getResourceAsStream("/" + filename));

    this.attachmentData.setFilename(filename);

    String fullText = this.attachmentData.getFullText(this.document, getContext());

    assertEquals("Wrong attachment content indexed", expect, fullText);
  }

  @Test
  public void testGetFullTextFromTxt() throws IOException {
    replayAll();
    assertGetFullText("text content\n", "txt.txt");
    verifyAll();
  }

  @Test
  public void testGetFullTextFromMSOffice97() throws IOException {
    replayAll();
    assertGetFullText("ms office 97 content\n\n", "msoffice97.doc");
    verifyAll();
  }

  @Test
  public void testGetFullTextFromOpenXML() throws IOException {
    replayAll();
    assertGetFullText("openxml content\n", "openxml.docx");
    verifyAll();
  }

  @Test
  public void testGetFullTextFromOpenDocument() throws IOException {
    replayAll();
    assertGetFullText("opendocument content\n", "opendocument.odt");
    verifyAll();
  }

  @Test
  public void testGetFullTextFromPDF() throws IOException {
    replayAll();
    assertGetFullText("\npdf content\n\n\n", "pdf.pdf");
    verifyAll();
  }

  @Test
  public void testGetFullTextFromZIP() throws IOException {
    replayAll();
    assertGetFullText("zip.txt\nzip content\n\n\n\n", "zip.zip");
    verifyAll();
  }


  private void replayAll(Object ... mocks) {
    replay(mocks);
    replay(servletContext);
  }

  private void verifyAll(Object ... mocks) {
    verify(mocks);
    verify(servletContext);
  }

}
