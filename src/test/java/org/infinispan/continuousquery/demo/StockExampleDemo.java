package org.infinispan.continuousquery.demo;

import org.infinispan.Cache;
import org.infinispan.continuousquery.ContinuousQuery;
import org.infinispan.continuousquery.ContinuousQueryManager;
import org.infinispan.continuousquery.QueryDefinition;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class StockExampleDemo {

   private static Log log = LogFactory.getLog(StockExampleDemo.class);
   private static final BufferedReader IN_READER = new BufferedReader(new InputStreamReader(System.in));

   private static final DefaultCacheManager cm = createCacheManager();
   private static ContinuousQueryManager cqm;
   private static Trader trader;


   public static void main(String[] args) throws Exception {
      String clusterSizeStr = args.length >= 2 ? args[0] : "3";
      String traderName = args.length >= 2 ? args[1] : "Nick" + System.currentTimeMillis();
      int clusterSize = Integer.parseInt(clusterSizeStr);
      cqm = waitForClusterToForm(clusterSize, cm);
      if (isCoordinator(cm)) {
         confirmToOthers(cm);
      }
      trader = new Trader(traderName);
      cm.getCache().put(trader.getName(), trader);
      System.out.println("Welcome " + traderName + " !");
      run();
   }


   private static void run() throws Exception {
      while (true) {
         printOptions();
         int option = readOption();
         try {
            switch (option) {
               case 0 :
                  defineQueries() ;
                  break;
               case 1:
                  listQueries();
                  break;
               case 2:
                  executeQuery(true);
                  break;
               case 3:
                  executeQuery(false);
                  break;
               case 4:
                  addStockInfo();
                  break;
               case 5:
                  showStockInfo();
                  break;
               case 6:
                  trade();
                  break;
               case 7:
                  System.exit(0);
               default:
                  System.err.println("Invalid option");
            }
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   private static void printOptions() {
      System.out.println("");
      System.out.println("0 - define queries");
      System.out.println("1 - list defined queries");
      System.out.println("2 - execute local query");
      System.out.println("3 - execute cluster query");
      System.out.println("4 - add stock info");
      System.out.println("5 - show stock info");
      System.out.println("6 - trade");
      System.out.println("7 - exit");
   }

   private static void trade() {
      String companyAndAmount = readInfo("Enter <company-stock-symbol>, <amount>");
      int separator = companyAndAmount.indexOf(",");
      String company = companyAndAmount.substring(0, separator);
      int amount = Integer.parseInt(companyAndAmount.substring(separator + 1).trim());
      Collection<Object> objects = cm.getCache().values();
      for (Object o : objects) {
         if (o instanceof StockInfo) {
            StockInfo stockInfo = (StockInfo) o;
            if (stockInfo.getCompany().equals(company)) {
               Trade trade = new Trade(trader, amount, stockInfo);
               cm.getCache().put(trade, trade);
               System.out.println("Trade added to the system: " + trade);
               return;
            }
         }
      }
      System.out.println("Cannot trade (no such company: " +company + ")");
   }

   private static void showStockInfo() {
      Collection<Object> objects = cm.getCache().values();
      String stockInfo = "";
      for (Object o : objects) {
         if (o instanceof StockInfo) {
            stockInfo +=  "   " + o + "\n";
         }
      }
      if (stockInfo.equals("")) {
         System.out.println("No stock info in the system.");
      } else {
         System.out.println("Stock info: " + stockInfo);
      }
   }

   private static void executeQuery(boolean local) {
      String query = readInfo("Enter " + (local ? "local" : "cluster") + " query ( <q name> (param1, param2...)");
      String qName = null;
      StringTokenizer qParams = null;
      try {
         qName = query.substring(0, query.indexOf('('));
         String paramsStr = query.substring(query.indexOf('(') + 1, query.indexOf(')'));
         qParams = new StringTokenizer(paramsStr, ",");
      } catch (Exception e) {
         System.err.println("Cannot add query: " + query + " because of: " + e.getMessage());
      }

      if (cqm.getDefinedQuery(qName) == null) {
         System.err.println("No such query: " + qName);
         return;
      }
      Object[] params = new Object[qParams.countTokens()];
      for (int i = 0; i < params.length; i++) {
         params[i] = qParams.nextToken();
      }
      cqm.executeContinuousQuery(qName, local, params);
   }

   private static void listQueries() {
      String queries = cqm.getDefinedQueries().size() == 0 ? "No queries defined" : "Defined queries are: ";
      for (QueryDefinition queryDefinition : cqm.getDefinedQueries()) {
         queries += queryDefinition.getQueryName();
         queries += "(";
         Iterator it = queryDefinition.getOutput().iterator();
         while (it.hasNext()) {
            queries += it.next();
            if (it.hasNext()) queries += ", ";
         }
         queries += ")  ";
      }
      System.out.println(queries);
   }

   private static void defineQueries() {
      String file = readInfo("Continuous 4" +
            "query definitions file");
      cqm.defineQueries(file);
      System.out.println("Queries successfully defined!");
   }

   private static int readOption() throws Exception {
      try {
         return Integer.parseInt(readInfo("Option"));
      } catch (Exception e) {
         return -1;
      }
   }

   private static void addStockInfo() throws Exception {
      String name = readInfo("Company name");
      double price = Double.parseDouble(readInfo("Stock price "));
      String profile = readInfo("Company profile ");
      StockInfo stockInfo = new StockInfo(name, price, profile);
      cm.getCache().remove(name);
      cm.getCache().put(name, stockInfo);
   }

   private static String readInfo(String s) {
      System.out.print(s + ": ");
      try {
         return IN_READER.readLine();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }


   private static boolean isCoordinator(DefaultCacheManager dcm) {
      return dcm.getCache().getAdvancedCache().getRpcManager().getTransport().isCoordinator();
   }

   private static DefaultCacheManager createCacheManager() {
      try {
         return new DefaultCacheManager("demo-config.xml", true);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private static ContinuousQueryManager waitForClusterToForm(int cacheCount, DefaultCacheManager dcm) throws InterruptedException {
      Cache cache = dcm.getCache();
      dcm.addListener(new TopologyChangeListener());
      ContinuousQueryManager result = new ContinuousQueryManager(dcm, true);
      TestingUtil.blockUntilViewReceived(cache, cacheCount, 1000000);
      TestingUtil.blockUntilCacheStatusAchieved(cache, ComponentStatus.RUNNING, 10000);
      Address address = cache.getAdvancedCache().getRpcManager().getAddress();
      List<Address> members = cache.getAdvancedCache().getRpcManager().getTransport().getMembers();
      Thread.sleep(1000);
      for (int i = 0; i < 30; i++) {
         cache.put(address, address);
         boolean containsAll = true;
         for (Address addr : members) {
            containsAll = containsAll && cache.containsKey(addr);
         }
         if (!containsAll) Thread.sleep(1000);
         else {
            waitForCoordinatorConfirmation(dcm, cache);
            return result;
         }
      }
      throw new IllegalStateException("Could not start cluster!!!");
   }

   private static void waitForCoordinatorConfirmation(DefaultCacheManager dcm, Cache cache) throws InterruptedException {
      if (!isCoordinator(dcm)) {
         while (cache.get("shallContinue") == null) {
            Thread.sleep(500);
         }
      }
   }

   @Listener
   public static final class TopologyChangeListener {

      @ViewChanged
      public void topologyChanged(ViewChangedEvent ev) {
         ArrayList removed = new ArrayList(ev.getOldMembers());
         removed.removeAll(ev.getNewMembers());
         if (!removed.isEmpty()) {
            System.err.println("\n Member(s) removed: " + removed);
         }
      }
   }

   private static void confirmToOthers(DefaultCacheManager dcm) {
      dcm.getCache().put("shallContinue", true);
   }
}
