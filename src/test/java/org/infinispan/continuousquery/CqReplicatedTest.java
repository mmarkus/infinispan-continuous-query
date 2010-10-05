package org.infinispan.continuousquery;

import org.infinispan.config.Configuration;
import org.infinispan.continuousquery.demo.*;
import org.infinispan.continuousquery.demo.StockInfo;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (testName = "continuousquery.demo.StockExampleClusteredTest")
public class CqReplicatedTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(CqReplicatedTest.class);

   private ContinuousQueryManager cqm1;
   private ContinuousQueryManager cqm2;
   private Random rnd;
   private final String daimler = "daimler";
   private final String dacia = "dacia";
   private Trader mircea;
   private Trader dan;
   private ContinuousQuery mirceaQ;
   private ContinuousQuery danQ;
   private static final String CQ_NAME = "traderTransactions";
   public static String LISTED_COMPANY_Q =
         "import org.infinispan.continuousquery.demo.Company " +
               "query listedCompanies(String $profile)  company : Company(profile == $profile) " +
               "end";
   public static String TRADER_TRANSACTIONS_Q =
         "      import org.infinispan.continuousquery.demo.Trader  " +
               "import org.infinispan.continuousquery.demo.Transaction  " +
               "import org.infinispan.continuousquery.demo.StockInfo  " +
               "query traderTransactions(String $traderName)  " +
               "    ourTrader: Trader(name == $traderName)  " +
               "    ourTrade : Trade(trader.name == ourTrader.name)  " +
               "    stockInfo : StockInfo(ourTransaction.company.stockSymbol == company.stockSymbol)  " +
               "end ";

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      registerCacheManager(TestCacheManagerFactory.createCacheManager(config));
      registerCacheManager(TestCacheManagerFactory.createCacheManager(config));
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));

      cqm1 = new ContinuousQueryManager(manager(0), true);
      cqm2 = new ContinuousQueryManager(manager(1), true);
   }

   @BeforeMethod
   @Override
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      rnd = new Random();

      mircea = new Trader("Mircea");
      dan = new Trader("Dan");

      cache(0).put("daimler", daimler);
      cache(0).put("dacia", dacia);

      cache(1).put("mircea", mircea);
      cache(1).put("dan", dan);

      cache(0).put("t1", new Trade(mircea, 1000, new StockInfo(daimler, 2.3, "CARS")));
      cache(1).put("t2", new Trade(dan, 500, new StockInfo(dacia, 5.1, "CARS")));

      QueryDefinition qDef = new QueryDefinition(CQ_NAME, TRADER_TRANSACTIONS_Q, Collections.singletonList("stockInfo"), null);

      log.info("before it starts");
      cqm1.defineQuery(qDef);

      assert cqm2.getDefinedQueries().contains(qDef);

      mirceaQ = cqm1.executeContinuousQuery(CQ_NAME, true, (Object) "Mircea");
      danQ = cqm2.executeContinuousQuery(CQ_NAME, true, (Object) "Dan");
   }

   public void testQueryHappens() {
      MoreComplexLocalApiTest.MyQueryListener mirceaListener = new MoreComplexLocalApiTest.MyQueryListener();
      MoreComplexLocalApiTest.MyQueryListener danListener = new MoreComplexLocalApiTest.MyQueryListener();
      mirceaQ.addQueryListener(mirceaListener);
      danQ.addQueryListener(danListener);
      List<StockInfo> daimlerSi = new ArrayList<StockInfo>();
      List<StockInfo> daciaSi = new ArrayList<StockInfo>();

      for (int i = 0; i < 100; i++) {
         StockInfo daimlerInfo = new StockInfo(daimler, rndDouble(2), "CARS");
         daimlerSi.add(daimlerInfo);
         cache(rndCache()).put("daimler" + i, daimlerInfo);
         assertEquals(daimlerSi, mirceaListener.stockInfo);

         StockInfo daciaInfo = new StockInfo(dacia, rndDouble(5), "CARS");
         daciaSi.add(daciaInfo);
         cache(rndCache()).put("dacia" + i, daciaInfo);
         assertEquals(daciaSi, danListener.stockInfo);
      }

      //now check removals
      for (int i = 99; i >= 0; i--) {
         cache(rndCache()).remove("daimler" + i);
         assertEquals(mirceaListener.stockInfo.size(), i);
      }

      cache(rndCache()).clear();
      assert mirceaListener.stockInfo.isEmpty();
      assert danListener.stockInfo.isEmpty();
   }

   public void testReplay() {
      List<StockInfo> daimlerSi = new ArrayList<StockInfo>();
      for (int i = 0; i < 100; i++) {
         StockInfo daimlerInfo = new StockInfo(daimler, rndDouble(2), "CARS");
         daimlerSi.add(daimlerInfo);
         cache(rndCache()).put("daimler" + i, daimlerInfo);
      }
      MoreComplexLocalApiTest.MyQueryListener mirceaListener = new MyReplayQueryListener();
      mirceaQ.addQueryListener(mirceaListener);
      assertEquals(daimlerSi, mirceaListener.stockInfo);
   }

   public void testClusterQueryFailover() {
      //todo add
   }
   
   public void testClusterQueryStopRequest() {
      //todo
   }

   public void testLifecycle() {

   }


   private double rndDouble(int start) {
      double rndDouble = rnd.nextDouble();
      return start + rndDouble;
   }


   private static class MyReplayQueryListener extends MoreComplexLocalApiTest.MyQueryListener implements ReplayResultSetListener {

      @Override
      public void replay(Iterator existingQueryResults) {
         while (existingQueryResults.hasNext()) {
            stockInfo.add(getStockInfo((MatchingEntry) existingQueryResults.next()));
         }
      }
   }

   private int rndCache() {
      return rnd.nextInt(2);
   }
}
