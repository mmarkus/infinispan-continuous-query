package org.infinispan.continuousquery.impl;

import org.infinispan.CacheException;
import org.infinispan.continuousquery.QueryDefinition;
import org.infinispan.util.FileLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class QueryDefinitionXmlParser {

   private static Log log = LogFactory.getLog(QueryDefinitionXmlParser.class);

   public List<QueryDefinition> parse(String fileName) {
      try {
         FileLookup fl = new FileLookup();
         InputStream inputStream = fl.lookupFile(fileName);
         DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
         DocumentBuilder db = dbf.newDocumentBuilder();
         Document doc = db.parse(inputStream);
         doc.getDocumentElement().normalize();
         List<QueryDefinition> defs = new ArrayList<QueryDefinition>();
         NodeList list = doc.getElementsByTagName("cq-definitions");
         if (list.getLength() == 0) {
            log.info("No definitions in " + fileName);
            return defs;
         }
         list = ((Element) list.item(0)).getElementsByTagName("continuous-query");
         for (int i = 0; i < list.getLength(); i++) {
            Element el = (Element) list.item(i);
            String queryName = el.getAttribute("name");
            String cacheName = el.getAttribute("cacheName");
            String defaultResultSetListener = el.getAttribute("defaultResultSetListener");
            String queryString = getQString(el);
            List<String> output = getOutput(el);
            QueryDefinition qd;
            qd = new QueryDefinition(queryName, queryString, nullifyEmptyString(cacheName), output, defaultResultSetListener);
            defs.add(qd);
         }
         return defs;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private String nullifyEmptyString(String cacheName) {
      return cacheName != null && cacheName.trim().length() == 0 ? null : cacheName;
   }

   private List<String> getOutput(Element el) {
      NodeList els = el.getElementsByTagName("output");
      if (els.getLength() == 0) {
         return Collections.EMPTY_LIST;
      }
      els = ((Element) els.item(0)).getElementsByTagName("item");
      List<String> result = new ArrayList<String>();
      for (int i = 0; i < els.getLength(); i++) {
         Element node = (Element) els.item(i);
         result.add(getItem(node));
      }
      return result;
   }

   private String getItem(Element node) {
      Node node1 = node.getChildNodes().item(0);
      return getText(node1);
   }

   private String getText(Node node1) {
      return ((Text) node1).getData();
   }

   private String getQString(Element el) {
      Element node = (Element) el.getElementsByTagName("query").item(0);
      NodeList childNodes = node.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
         Node node1 = childNodes.item(i);
         if (node1 instanceof CDATASection) {
            return ((CDATASection)node1).getData();
         }
      }
      throw new IllegalStateException("Missing query!");
   }
}
