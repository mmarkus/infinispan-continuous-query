package org.infinispan.continuousquery;

import org.infinispan.continuousquery.impl.QueryDefinitionXmlParser;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "continuousquery.QueryDefinitionParserTest")
public class QueryDefinitionParserTest {

   public void testParseFile() {
      List<QueryDefinition> list = new QueryDefinitionXmlParser().parse("cqs.xml");
      assert list.size() == 2;
      QueryDefinition qd1 = list.get(0);
      assert qd1.getCacheName() == null;
      assertEquals(qd1.getQueryName(), "listedCompanies");
      assertEquals(qd1.getOutput().size(), 1);
      assertEquals(qd1.getOutput().get(0), "company");
      assert qd1.getQueryStr().indexOf("String $profile") > 0;
      assertEquals(qd1.getDefaultResultSetListener(), "org.infinispan.continuousquery.demo.CompanyResultSetListener");

      QueryDefinition qd2 = list.get(1);
      assert qd2.getCacheName() == null;
      assertEquals(qd2.getQueryName(), "traderTransactions");
      assertEquals(qd2.getOutput().size(), 3);
      assert qd2.getOutput().contains("ourTrader");
      assert qd2.getOutput().contains("ourTrade");
      assert qd2.getOutput().contains("stockInfo");
   }
}
