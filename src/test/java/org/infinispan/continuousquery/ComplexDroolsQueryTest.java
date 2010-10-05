package org.infinispan.continuousquery;

import org.infinispan.config.Configuration;
import org.infinispan.continuousquery.demo.StockInfo;
import org.infinispan.continuousquery.demo.Trade;
import org.infinispan.continuousquery.demo.Trader;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional")
public class ComplexDroolsQueryTest extends SingleCacheManagerTest {
   
      public static final String ALL_STOCKS_Q =
            "         import org.infinispan.continuousquery.demo.*\n" +
            "         query traderTransactions(String $traderName)\n" +
            "            ourTrader: Trader(name == $traderName)\n" +
            "            ourTrade : Trade(trader.name == ourTrader.name)\n" +
            "            stockInfo : StockInfo(ourTrade.stockInfo.company == company)\n" +
            "         end";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.LOCAL));
   }

   public void testDefineAndRunLocalQuery() {
      ContinuousQueryManager cm = new ContinuousQueryManager(cacheManager, true);
      QueryDefinition qDef = new QueryDefinition("traderTransactions", ALL_STOCKS_Q, Collections.singletonList("stockInfo"));
      cm.defineQuery(qDef);

      ContinuousQuery cq = cm.executeContinuousQuery(qDef.getQueryName(), true, new Object[] {"Mircea"});
      MyQueryListener listener = new MyQueryListener();
      cq.addQueryListener(listener);

      Trader trader = new Trader("Mircea");
      StockInfo stockInfo = new StockInfo("dacia", 2.3, "cars");
      Trade trade = new Trade(trader, 10, stockInfo);
      cacheManager.getCache().put(trader, trader);
      cacheManager.getCache().put(stockInfo, stockInfo);
      cacheManager.getCache().put(trade, trade);

      assert listener.addedCompanies.size() > 0;
      assert listener.addedCompanies.contains("dacia");

   }

   public void testDefineAndRunClusterQuery() {
      //todo re enable
//      defineAndRunLocalQuery(false);
   }

   public static class MyQueryListener implements ReplayResultSetListener {

      List<String> existingCompanies = new ArrayList<String>();

      List<String> addedCompanies = new ArrayList<String>();

      List<String> removedCompanies = new ArrayList<String>();

      List<String> updatedCompanies = new ArrayList<String>();

      @Override
      public void replay(Iterator<MatchingEntry> existingQueryResults) {
         while (existingQueryResults.hasNext()) {
            MatchingEntry map = existingQueryResults.next();
            existingCompanies.add(comp(map));
         }
      }

      @Override
      public void entryAdded(MatchingEntry row) {
         addedCompanies.add(comp(row));
      }

      @Override
      public void entryRemoved(MatchingEntry row) {
         removedCompanies.add(comp(row));
      }

      @Override
      public void entryUpdated(MatchingEntry row) {
         updatedCompanies.add(comp(row));
      }

      private String comp(MatchingEntry map) {
         return ((StockInfo) map.get("stockInfo")).getCompany();
      }
   }
}
