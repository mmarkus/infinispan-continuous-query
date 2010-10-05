package org.infinispan.continuousquery;

import org.infinispan.config.Configuration;
import org.infinispan.continuousquery.demo.StockInfo;
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
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test
public class ContinuousQueryLocalApiTest extends SingleCacheManagerTest {

   public static final String ALL_STOCKS_Q =
      " import org.infinispan.continuousquery.demo.* \n"
      + "query allCompanies() \n"
      + "    stockInfo : StockInfo() \n"
      + "end\n";
   private static final String LIST_STOCK_Q = "";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.LOCAL));
   }

   public void testDefineAndRunLocalQuery() {
      defineAndRunLocalQuery(true);
   }

   public void testDefineAndRunClusterQuery() {
      //todo re enable
//      defineAndRunLocalQuery(false);
   }

   private void defineAndRunLocalQuery(boolean local) {

      ContinuousQueryManager cm = new ContinuousQueryManager(cacheManager, true);
      QueryDefinition qDef = new QueryDefinition("allCompanies", ALL_STOCKS_Q, Collections.singletonList("stockInfo"));
      cm.defineQuery(qDef);

      StockInfo compA = new StockInfo("a", 2.3, "prof");
      cache.put("a", compA);

      ContinuousQuery cq = cm.executeContinuousQuery(qDef.getQueryName(), local);
      assert cq.getQueryListeners().isEmpty();

      MyQueryListener listener = new MyQueryListener();
      cq.addQueryListener(listener);
      assertEquals(Collections.singletonList(compA.getCompany()), listener.existingCompanies);
      assert cq.getQueryListeners().contains(listener);

      StockInfo compB = new StockInfo("b", 3.4, "dsa");
      cache.put("b", compB);
      assertEquals(Collections.singletonList(compB.getCompany()), listener.addedCompanies);

      List<MatchingEntry> result = cq.getQueryResult();
      assert result.size() == 2;
      assert cq.contains(compA, "stockInfo");
      assert cq.contains(compB, "stockInfo");

      StockInfo compC = new StockInfo("new_b", 4, "dadsa");
      cache.put("b", compC);
      assertEquals(Collections.singletonList(compB.getCompany()), listener.removedCompanies);
      result = cq.getQueryResult();
      assert cq.contains(compA, "stockInfo");
      assert cq.contains(compC, "stockInfo");
      assert result.size() == 2;


      cache.remove("a");
      List removed = Arrays.asList(compB.getCompany(), compA.getCompany());
      assertEquals(listener.removedCompanies, removed);
      result = cq.getQueryResult();
      assert cq.contains(compC, "stockInfo");
      assert result.size() == 1;

      assert cq.removeQueryListener(listener);
      assert cq.getQueryListeners().isEmpty();

      cq.close();
      try {
         cq.addQueryListener(new MyQueryListener());
         assert false;
      } catch (IllegalStateException e) {
         e.printStackTrace();
      }
      cm.resetData();
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
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
