package org.infinispan.continuousquery;

import org.infinispan.config.Configuration;
import org.infinispan.continuousquery.demo.*;
import org.infinispan.continuousquery.demo.StockInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test
public class MoreComplexLocalApiTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.LOCAL));
   }

   public void testCq() {
      ContinuousQueryManager cm = new ContinuousQueryManager(cacheManager, true);
      QueryDefinition qDef = new QueryDefinition("traderTransactions", CqReplicatedTest.TRADER_TRANSACTIONS_Q, Collections.singletonList("ourTransaction"));
      cm.defineQuery(qDef);

      String daimler = "Daimler";
      Trader mircea = new Trader("Mircea");

      cache.put("daimler", daimler);
      cache.put("dacia", daimler);

      cache.put("mircea", mircea);

      cache.put("t1", new Trade(mircea, 1000, new StockInfo(daimler, 2.3, "CARS")));

      ContinuousQuery mirceaQ = cm.executeContinuousQuery("traderTransactions", true, (Object) "Mircea");
      MyQueryListener mirceaListener = new MyQueryListener();
      mirceaQ.addQueryListener(mirceaListener);
      List<StockInfo> daimlerSi = new ArrayList<StockInfo>();

      for (int i = 0; i < 100; i++) {
         StockInfo daimlerInfo = new StockInfo(daimler, 0.2, "CARS");
         daimlerSi.add(daimlerInfo);
         cache.put("daimler" + i, daimlerInfo);
         assertEquals(daimlerSi, mirceaListener.stockInfo);
      }
   }

   public static class MyQueryListener implements ResultSetListener {
      public final List<StockInfo> stockInfo;
      public StockInfo updated;

      public MyQueryListener() {
         this.stockInfo = new ArrayList<StockInfo>();
      }

      @Override
      public void entryAdded(MatchingEntry row) {
         stockInfo.add(getStockInfo(row));
      }


      @Override
      public void entryRemoved(MatchingEntry row) {
         stockInfo.remove(getStockInfo(row));
      }

      @Override
      public void entryUpdated(MatchingEntry row) {
         updated = getStockInfo(row);
      }

      protected StockInfo getStockInfo(MatchingEntry row) {
         return (StockInfo) row.get("stockInfo");
      }
   }


}
