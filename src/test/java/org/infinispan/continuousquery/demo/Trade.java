package org.infinispan.continuousquery.demo;

import java.io.Serializable;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class Trade implements Serializable {

   private Trader trader;
   private int quantity;
   private StockInfo stockInfo;

   public Trade(Trader trader, int quantity, StockInfo stockInfo) {
      this.trader = trader;
      this.quantity = quantity;
      this.stockInfo = stockInfo;
   }

   public String getCompany() {
      return stockInfo.getCompany();
   }

   public Trader getTrader() {
      return trader;
   }

   public int getQuantity() {
      return quantity;
   }

   public double getStockUnitValue() {
      return stockInfo.getStockUnitValue();
   }


   public StockInfo getStockInfo() {
      return stockInfo;
   }

   /**
    * If stock goes more than 10% down then sell at all means!
    */
   public boolean isOfConcern(StockInfo si) {
      return 0.9 * getStockUnitValue() > si.getStockUnitValue();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Trade trade = (Trade) o;

      if (quantity != trade.quantity) return false;
      if (stockInfo != null ? !stockInfo.equals(trade.stockInfo) : trade.stockInfo != null) return false;
      if (trader != null ? !trader.equals(trade.trader) : trade.trader != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = trader != null ? trader.hashCode() : 0;
      result = 31 * result + quantity;
      result = 31 * result + (stockInfo != null ? stockInfo.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "Trade{" +
            "trader=" + trader +
            ", quantity=" + quantity +
            ", stockInfo=" + stockInfo +
            '}';
   }
}
