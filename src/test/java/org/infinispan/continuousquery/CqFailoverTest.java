package org.infinispan.continuousquery;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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
public class CqFailoverTest extends MultipleCacheManagersTest {
   private static Log log = LogFactory.getLog(CqFailoverTest.class);

   private ContinuousQueryManager cqm1;
   private ContinuousQueryManager cqm2;
   private ContinuousQueryManager cqm3;

   private static List<String> companies = new ArrayList<String>();

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC));
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC));
      EmbeddedCacheManager cm3 = TestCacheManagerFactory.createCacheManager(getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC));
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      registerCacheManager(cm3);
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1), cache(2));
      cache(2).put("k","v");
      assertEquals("v", cache(2).get("k"));
      assertEquals("v", cache(1).get("k"));
      assertEquals("v", cache(0).get("k"));


      cqm1 = new ContinuousQueryManager(cm1, true);
      cqm2 = new ContinuousQueryManager(cm2, true);
      cqm3 = new ContinuousQueryManager(cm3, true);

      QueryDefinition def = new QueryDefinition("listedCompanies", CqReplicatedTest.LISTED_COMPANY_Q, Collections.singletonList("company"));

      log.info("___here ");
      cqm3.defineQuery(def);

      assert cqm1.getDefinedQueries().contains(def);
      assert cqm2.getDefinedQueries().contains(def);
      assert cqm3.getDefinedQueries().contains(def);
   }

   public void testFailover() {
      ContinuousQuery continuousQuery = cqm3.executeContinuousQuery("listedCompanies", false, (Object) "TECH");
      continuousQuery.addQueryListener(new CompanyQueryListener());
      cache(0).put("a", comp("a"));
      assertAdded("a",1);
      cache(1).put("b", comp("b"));
      assertAdded("b",2);
      cache(2).put("c", comp("c"));
      assertAdded("c",3);

      cache(2).stop();
      TestingUtil.killCacheManagers(manager(2));
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));

      cache(1).stop();
      TestingUtil.killCacheManagers(manager(1));
      TestingUtil.blockUntilViewsReceived(10000, cache(0));

      cache(0).put("e", comp("e"));
      assertAdded("e", 4);
   }

   private void assertAdded(String compName, int size) {
      assertEquals(companies.size(), size);
      assert companies.contains(comp(compName));
   }

   private String comp(String name) {
      return name;
   }

   public static class CompanyQueryListener implements ResultSetListener {

      @Override
      public void entryAdded(MatchingEntry row) {
         companies.add(getCompany(row));
      }

      @Override
      public void entryRemoved(MatchingEntry row) {
         companies.remove(getCompany(row));
      }

      @Override
      public void entryUpdated(MatchingEntry row) {
         // TODO: Customise this generated block
      }

      private String getCompany(MatchingEntry row) {
         return (String) row.get("company");
      }
   }
}
