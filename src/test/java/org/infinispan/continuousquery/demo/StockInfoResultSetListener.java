package org.infinispan.continuousquery.demo;

import org.infinispan.continuousquery.MatchingEntry;
import org.infinispan.continuousquery.ResultSetListenerSupport;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class StockInfoResultSetListener extends ResultSetListenerSupport {

   @Override
   public void entryAdded(MatchingEntry row) {
      Trade trade = (Trade) row.get("ourTrade");
      StockInfo si = (StockInfo) row.get("stockInfo");
      double difference = si.getStockUnitValue() - trade.getStockUnitValue();
      if (trade.isOfConcern(si)) {
         printWarningMessage(trade, difference);
      } else {
         printUpdateMessage(trade, difference);
      }
   }

   private void printUpdateMessage(Trade trade, double difference) {
      NumberFormat nf = new DecimalFormat();
      if (difference > 0) {
         String diffStr = nf.format(difference);
         System.out.println("Not bad! Our stock on " + trade.getCompany() + " is up with " + diffStr + " unit(s)!");
      } else if (difference < 0) {
         System.out.println(trade + " is down with " + (-difference));
      } else {
         System.out.println(trade + "hasn't changed value");
      }
   }

   private void printWarningMessage(Trade trade, double difference) {
      NumberFormat nf = new DecimalFormat();
      System.out.println(trade.getTrader().getName() + "'s transaction on " + trade.getCompany() + " went down " + nf.format(-difference) + ". This needs immediate attention!");
   }
}